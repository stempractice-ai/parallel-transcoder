use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::TempDir;
use tokio::process::Command;
use tracing::{debug, error, info, warn};
use transcoder_cluster::media::{self, MediaSummary, Recommendation};

mod analyzer;
mod segmenter;

#[derive(Parser, Debug)]
#[command(name = "transcoder-coordinator")]
#[command(about = "Coordinator for parallel video transcoding", long_about = None)]
struct Args {
    /// Input video file path (optional; typically supplied by the GUI)
    #[arg(short, long)]
    input: Option<String>,

    /// Output path (directory for HLS, file for MP4; optional, typically supplied by the GUI)
    #[arg(short, long)]
    output: Option<String>,

    /// Number of worker processes (0 = auto-detect CPU cores)
    #[arg(short, long, default_value = "0")]
    workers: usize,

    /// Target segment duration in seconds
    #[arg(short, long, default_value = "10.0")]
    segment_duration: f64,

    /// Output format: hls or mp4
    #[arg(short, long, default_value = "hls")]
    format: String,

    /// CRF value for quality-based encoding
    #[arg(short, long, default_value = "23")]
    crf: u32,

    /// Encoding preset
    #[arg(short, long, default_value = "medium")]
    preset: String,

    /// Encoder to use (libx264, libx265, libsvtav1, libaom-av1, h264_videotoolbox, hevc_videotoolbox)
    #[arg(short = 'E', long, default_value = "libx264")]
    encoder: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Fast mode: use ffprobe analysis + pre-split segments (skips full decode analysis)
    #[arg(long)]
    fast: bool,

    /// Copy mode: stream-copy all segments without re-encoding (implies --fast)
    #[arg(long)]
    copy: bool,

    /// Smart mode: per-segment copy-vs-encode decision based on GOP bitrate analysis (implies --fast)
    #[arg(long)]
    smart: bool,

    /// Bitrate tolerance for smart mode (0.0-1.0, default 0.3 = ±30%)
    #[arg(long, default_value = "0.3")]
    smart_tolerance: f64,

    /// Smart auto-tune: automatically compute tolerance from GOP bitrate distribution (implies --smart)
    #[arg(long)]
    smart_auto: bool,

    /// Smart report: dry-run analysis showing per-segment copy/encode decisions as JSON (implies --smart)
    #[arg(long)]
    smart_report: bool,

    /// Enable distributed cluster mode
    #[arg(long)]
    cluster: bool,

    /// Cluster master address to submit job to (e.g., 192.168.1.10:9900)
    #[arg(long)]
    cluster_master: Option<String>,
}

/// Segment descriptor sent to the worker process (matches worker's SegmentDescriptor).
#[derive(Debug, Serialize)]
struct SegmentDescriptor {
    id: usize,
    start_frame: u64,
    end_frame: u64,
    start_timestamp: f64,
    end_timestamp: f64,
    lookahead_frames: Option<usize>,
    complexity_estimate: f32,
    scene_changes: Vec<u64>,
}

/// Result returned by a worker process via stdout JSON.
#[derive(Debug, Deserialize)]
struct WorkerResult {
    segment_id: usize,
    frames_encoded: u64,
    output_size_bytes: u64,
    encoding_time_secs: f64,
    output_path: String,
}

/// Smart mode report output (JSON to stdout).
#[derive(Debug, Serialize)]
struct SmartReport {
    input: String,
    segments: Vec<SegmentReport>,
    summary: ReportSummary,
    media: MediaSummary,
    recommendation: Recommendation,
}

#[derive(Debug, Serialize)]
struct SegmentReport {
    id: usize,
    start_time: f64,
    end_time: f64,
    decision: String, // "copy" or "encode"
    bitrate_ratio: Option<f64>,
    reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ReportSummary {
    total_segments: usize,
    copy_segments: usize,
    encode_segments: usize,
    global_blockers: Vec<String>,
}

impl From<&segmenter::Segment> for SegmentDescriptor {
    fn from(seg: &segmenter::Segment) -> Self {
        SegmentDescriptor {
            id: seg.id,
            start_frame: seg.start_frame,
            end_frame: seg.end_frame,
            start_timestamp: seg.start_timestamp,
            end_timestamp: seg.end_timestamp,
            lookahead_frames: None,
            complexity_estimate: seg.complexity_estimate,
            scene_changes: seg.contains_scene_changes.clone(),
        }
    }
}

/// The shared copy/encode mode name for these flags. Smart flags win over
/// --copy, as they always have; --smart-report plans as plain smart.
fn mode_from_args(args: &Args) -> &'static str {
    if args.smart_auto {
        "smart-auto"
    } else if args.smart || args.smart_report {
        "smart"
    } else if args.copy {
        "copy"
    } else {
        "normal"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let log_level = if args.verbose { "debug" } else { "info" };
    // Logs go to stderr: stdout carries the --smart-report JSON, which the
    // web server parses whole.
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_writer(std::io::stderr)
        .init();

    // --copy, --smart, --smart-auto, --smart-report imply --fast (need ffprobe + pre-split)
    let smart = args.smart || args.smart_auto || args.smart_report;
    let fast = args.fast || args.copy || smart;

    info!("Parallel Video Transcoder Coordinator v0.1.0");
    if args.copy {
        info!("Mode: COPY (stream-copy, no re-encoding)");
    } else if smart {
        info!("Mode: SMART (per-segment copy-or-encode, tolerance=±{:.0}%)", args.smart_tolerance * 100.0);
    }
    let input_arg = args.input.as_deref()
        .context("--input is required (provide it on the command line or via the GUI)")?;
    let output_arg = args.output.as_deref()
        .context("--output is required (provide it on the command line or via the GUI)")?;

    info!("Input: {}", input_arg);
    info!("Output: {}", output_arg);

    let num_workers = if args.workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        args.workers
    };
    if !args.copy {
        info!("Using {} worker processes", num_workers);
    }

    let input_path = std::fs::canonicalize(input_arg)
        .with_context(|| format!("Input file not found: {}", input_arg))?;

    // Phase 1 — Video Analysis
    info!("Step 1: Analyzing video{}...", if fast { " (fast mode)" } else { "" });
    let analysis_start = Instant::now();
    let metadata = if fast {
        media::fast_analyze_video(input_path.to_str().unwrap())?
    } else {
        analyzer::analyze_video(input_path.to_str().unwrap())?
    };
    info!("  Analysis completed in {:.2}s", analysis_start.elapsed().as_secs_f64());
    info!(
        "  Video: {}x{} @ {:.2} fps, {:.2}s, {} total frames",
        metadata.width, metadata.height, metadata.fps, metadata.duration_secs, metadata.total_frames
    );
    info!(
        "  Found {} keyframes, {} scene changes",
        metadata.keyframe_positions.len(),
        metadata.scene_changes.len()
    );
    if !metadata.audio_tracks.is_empty() {
        info!("  Audio: {} tracks", metadata.audio_tracks.len());
    }
    if !metadata.subtitle_tracks.is_empty() {
        info!("  Subtitles: {} tracks ({} text-based)",
            metadata.subtitle_tracks.len(),
            metadata.subtitle_tracks.iter().filter(|t| t.is_text_based).count());
    }
    if !metadata.chapters.is_empty() {
        info!("  Chapters: {}", metadata.chapters.len());
    }

    // Phase 2 — Segmentation
    info!("Step 2: Creating segments...");
    let segments = segmenter::create_segments(&metadata, args.segment_duration)?;
    info!("  Created {} segments", segments.len());
    for seg in &segments {
        info!(
            "  Segment {}: frames {}-{} ({:.2}s-{:.2}s) complexity={:.3}",
            seg.id, seg.start_frame, seg.end_frame, seg.start_timestamp, seg.end_timestamp, seg.complexity_estimate
        );
    }

    // Distributed cluster mode: submit job to cluster master and monitor remotely
    if args.cluster {
        return run_cluster_mode(&args, &input_path, &metadata, &segments).await;
    }

    // Prepare output directory
    let output_dir = Path::new(output_arg);
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {}", output_arg))?;

    // Phase 2.5 — Pre-split segments (fast or copy mode)
    // Creates small .ts files per segment via stream copy, so each worker reads only its portion.
    // In copy mode, these pre-split files ARE the final output segments.
    let presplit_dir: Option<TempDir> = if fast && !args.copy {
        // Fast mode: pre-split into temp dir (workers will re-encode)
        Some(TempDir::new().context("Failed to create temp dir for pre-split files")?)
    } else {
        None
    };

    // In copy mode, pre-split directly into the output directory
    let presplit_target_dir = if args.copy {
        output_dir
    } else if let Some(ref td) = presplit_dir {
        td.path()
    } else {
        output_dir // won't be used, but need a value
    };

    let has_text_subs = metadata.subtitle_tracks.iter().any(|t| t.is_text_based);

    // The report never uses the pre-split files, so --smart-report skips the split.
    let presplit_paths: Option<Vec<PathBuf>> = if fast && !args.smart_report {
        info!("Step 2.5: {}splitting {} segments...",
            if args.copy { "Stream-copy " } else { "Pre-" },
            segments.len()
        );
        let presplit_start = Instant::now();
        let split_pb = ProgressBar::new(segments.len() as u64);
        split_pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {percent:>3}% • {pos}/{len} segments • ETA {eta_precise}")
                .unwrap()
                .progress_chars("##-"),
        );
        let paths = presplit_segments(
            input_path.to_str().unwrap(),
            &segments,
            presplit_target_dir,
            args.copy, // use segment_NNNN.ts naming in copy mode
            Some(&split_pb),
            has_text_subs,
        ).await?;
        split_pb.finish_with_message("splitting complete");
        info!("  {}split completed in {:.2}s",
            if args.copy { "Stream-copy " } else { "Pre-" },
            presplit_start.elapsed().as_secs_f64()
        );
        Some(paths)
    } else {
        None
    };

    let pipeline_start = Instant::now();

    // --- Smart mode: decide copy vs encode per segment ---
    let mode = mode_from_args(&args);
    let ranges: Vec<(f64, f64)> = segments.iter().map(|s| (s.start_timestamp, s.end_timestamp)).collect();
    let plan = media::plan_segments(&metadata, &ranges, mode, &args.encoder, args.crf, args.smart_tolerance);
    let smart_tolerance = plan.tolerance;

    if smart {
        if args.smart_auto {
            info!("Smart auto-tune: computed tolerance = ±{:.1}% (max ±{:.0}%)",
                smart_tolerance * 100.0, args.smart_tolerance * 100.0);
        }
        if !plan.blockers.is_empty() {
            info!("Smart mode global compatibility issues:");
            for b in &plan.blockers {
                match b {
                    media::CopyBlocker::CodecMismatch { source, target } =>
                        info!("  Codec mismatch: source={}, target={}", source, target),
                    media::CopyBlocker::ProfileIncompatible { source, target } =>
                        info!("  Profile incompatible: source={}, target={}", source, target),
                    media::CopyBlocker::PixFmtMismatch { source, target } =>
                        info!("  Pixel format mismatch: source={}, target={}", source, target),
                }
            }
        }

        let target_codec = media::encoder_to_codec(&args.encoder);
        info!("Smart mode analysis:");
        info!("  Source codec: {}, target codec: {} (match={})",
            metadata.codec_name, target_codec, target_codec == metadata.codec_name);
        info!("  Estimated target bitrate: {:.1} Mbps (tolerance=±{:.0}%)",
            plan.target_bitrate / 1_000_000.0, smart_tolerance * 100.0);

        if !plan.blockers.is_empty() {
            info!("  Global blocker detected — all segments will be re-encoded");
        } else {
            for (i, seg) in segments.iter().enumerate() {
                match plan.bitrate_ratio[i] {
                    None => debug!("  Segment {}: ENCODE (no GOP data)", seg.id),
                    Some(ratio) if plan.copy[i] => info!("  Segment {}: COPY (bitrate {:.1} Mbps, ratio={:.2})",
                        seg.id, ratio * plan.target_bitrate / 1_000_000.0, ratio),
                    Some(ratio) => info!("  Segment {}: ENCODE (bitrate {:.1} Mbps, ratio={:.2}, out of ±{:.0}% range)",
                        seg.id, ratio * plan.target_bitrate / 1_000_000.0, ratio, smart_tolerance * 100.0),
                }
            }
        }
    }

    let segment_decisions = &plan.copy;
    let copy_count = segment_decisions.iter().filter(|&&d| d).count();
    let encode_count = segment_decisions.iter().filter(|&&d| !d).count();

    if smart {
        info!("Smart decision: {} copy, {} encode out of {} segments",
            copy_count, encode_count, segments.len());
    }

    // Smart report mode: print JSON report and exit
    if args.smart_report {
        let global_blockers: Vec<String> = plan.blockers.iter().map(media::describe_blocker).collect();

        let segment_reports: Vec<SegmentReport> = segments.iter().enumerate().map(|(i, seg)| {
            let ratio = plan.bitrate_ratio[i];
            let mut reasons = global_blockers.clone();
            if let Some(r) = ratio {
                if !segment_decisions[i] && (r < (1.0 - smart_tolerance) || r > (1.0 + smart_tolerance)) {
                    reasons.push(format!("bitrate ratio {:.2} outside ±{:.0}%", r, smart_tolerance * 100.0));
                }
            } else {
                reasons.push("no GOP data".to_string());
            }

            SegmentReport {
                id: seg.id,
                start_time: seg.start_timestamp,
                end_time: seg.end_timestamp,
                decision: if segment_decisions[i] { "copy".to_string() } else { "encode".to_string() },
                bitrate_ratio: ratio,
                reasons,
            }
        }).collect();

        let report = SmartReport {
            input: input_arg.to_string(),
            segments: segment_reports,
            summary: ReportSummary {
                total_segments: segments.len(),
                copy_segments: copy_count,
                encode_segments: encode_count,
                global_blockers: global_blockers.clone(),
            },
            media: MediaSummary::from(&metadata),
            recommendation: media::recommend(&metadata, &args.encoder, args.crf, args.smart_tolerance),
        };

        // JSON to stdout
        println!("{}", serde_json::to_string_pretty(&report)?);
        // Human-readable summary to stderr
        eprintln!("Smart Report: {} total segments, {} copy, {} encode",
            segments.len(), copy_count, encode_count);
        if !global_blockers.is_empty() {
            eprintln!("Global blockers: {}", global_blockers.len());
        }
        return Ok(());
    }

    // Build results: copy segments get WorkerResult directly, encode segments go to workers
    let mut completed: Vec<WorkerResult> = Vec::new();
    let mut failures: Vec<(usize, String)> = Vec::new();

    // Handle copied segments (immediate — no workers needed)
    if let Some(ref paths) = presplit_paths {
        for (i, seg) in segments.iter().enumerate() {
            if segment_decisions[i] {
                let path = &paths[i];
                // In smart mode, copy pre-split file to output dir with final naming
                let final_path = if smart {
                    let dest = output_dir.join(format!("segment_{:04}.ts", seg.id));
                    std::fs::copy(path, &dest)
                        .with_context(|| format!("Failed to copy segment {} to output", seg.id))?;
                    dest
                } else {
                    // In copy mode, files are already in output dir
                    path.clone()
                };

                let size = std::fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
                let frames = seg.end_frame.saturating_sub(seg.start_frame) + 1;
                completed.push(WorkerResult {
                    segment_id: seg.id,
                    frames_encoded: frames,
                    output_size_bytes: size,
                    encoding_time_secs: 0.0,
                    output_path: final_path.to_string_lossy().to_string(),
                });
            }
        }
    }

    // Handle encoded segments (spawn workers)
    if encode_count > 0 {
        let worker_bin = find_worker_binary()?;
        info!("  Worker binary: {}", worker_bin.display());
        info!("Step 3: Transcoding {} segments with {} workers...", encode_count, num_workers);

        let multi_progress = MultiProgress::new();
        let overall_pb = multi_progress.add(ProgressBar::new(encode_count as u64));
        overall_pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {percent:>3}% • {pos}/{len} segments • ETA {eta_precise}")
                .unwrap()
                .progress_chars("##-"),
        );

        // Build descriptors only for segments that need encoding
        let encode_descriptors: Vec<(usize, SegmentDescriptor)> = segments
            .iter()
            .enumerate()
            .filter(|(i, _)| !segment_decisions[*i])
            .map(|(i, seg)| (i, SegmentDescriptor::from(seg)))
            .collect();

        let use_hw_decode = fast
            && (args.encoder.contains("videotoolbox") || args.encoder.contains("nvenc") || args.encoder.contains("vaapi"));

        let results: Vec<Result<WorkerResult>> = stream::iter(encode_descriptors.into_iter().enumerate())
            .map(|(worker_id, (seg_idx, desc))| {
                let input_path = input_path.clone();
                let output_dir = output_dir.to_path_buf();
                let worker_bin = worker_bin.clone();
                let verbose = args.verbose;
                let crf = args.crf;
                let preset = args.preset.clone();
                let encoder = args.encoder.clone();
                let pb = overall_pb.clone();
                let mp = multi_progress.clone();
                let presplit_path = presplit_paths
                    .as_ref()
                    .map(|paths| paths[seg_idx].clone());
                async move {
                    let result = spawn_worker(
                        &worker_bin,
                        worker_id,
                        &desc,
                        &input_path,
                        &output_dir,
                        crf,
                        &preset,
                        &encoder,
                        verbose,
                        presplit_path.as_deref(),
                        use_hw_decode,
                        &mp,
                    )
                    .await;
                    pb.inc(1);
                    result
                }
            })
            .buffer_unordered(num_workers)
            .collect()
            .await;

        overall_pb.finish_with_message("all segments complete");

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(wr) => {
                    info!(
                        "  Segment {} done: {} frames, {:.1}s, {} bytes",
                        wr.segment_id, wr.frames_encoded, wr.encoding_time_secs, wr.output_size_bytes
                    );
                    completed.push(wr);
                }
                Err(e) => {
                    error!("  Segment {} failed: {:#}", i, e);
                    failures.push((i, format!("{:#}", e)));
                }
            }
        }
    }

    if copy_count > 0 && encode_count > 0 {
        info!("Smart mode: {} copied + {} encoded = {} total segments",
            copy_count, encode_count, segments.len());
    } else if copy_count > 0 {
        info!("Stream-copy complete: {} segments", copy_count);
    }

    if !failures.is_empty() {
        for (id, err) in &failures {
            error!("  Failed segment {}: {}", id, err);
        }
        anyhow::bail!(
            "{} of {} segments failed to transcode",
            failures.len(),
            completed.len() + failures.len()
        );
    }

    let pipeline_elapsed = pipeline_start.elapsed();

    // Phase 4 — Reassemble Output
    info!("Step 4: Assembling output...");
    let mut completed = completed;
    completed.sort_by_key(|r| r.segment_id);

    // Extract subtitles as WebVTT for HLS
    let subtitle_vtt_files = if args.format == "hls" && metadata.subtitle_tracks.iter().any(|t| t.is_text_based) {
        info!("Extracting subtitle tracks as WebVTT...");
        extract_subtitles_as_webvtt(
            input_path.to_str().unwrap(),
            &metadata.subtitle_tracks,
            output_dir,
        ).await?
    } else {
        vec![]
    };

    match args.format.as_str() {
        "hls" => {
            let playlist_path = output_dir.join("playlist.m3u8");
            generate_hls_playlist(&playlist_path, &completed, &segments, metadata.fps,
                &metadata.audio_tracks, &subtitle_vtt_files)?;
            info!("  HLS playlist written to {}", playlist_path.display());
        }
        "mp4" => {
            let mp4_path = output_dir.join("output.mp4");
            let has_audio = !metadata.audio_tracks.is_empty();
            let audio_source = if has_audio { Some(input_path.to_str().unwrap()) } else { None };
            concatenate_mp4(&mp4_path, &completed, &metadata.chapters, audio_source)?;
            info!("  MP4 output written to {}", mp4_path.display());
        }
        other => {
            anyhow::bail!("Unknown output format: {}. Use 'hls' or 'mp4'.", other);
        }
    }

    // Summary
    let total_frames: u64 = completed.iter().map(|r| r.frames_encoded).sum();
    let total_bytes: u64 = completed.iter().map(|r| r.output_size_bytes).sum();
    info!("Pipeline complete:");
    info!("  Total frames: {}", total_frames);
    info!("  Total output size: {:.2} MB", total_bytes as f64 / 1_048_576.0);
    info!("  Wall time: {:.1}s", pipeline_elapsed.as_secs_f64());
    info!(
        "  Throughput: {:.1} fps",
        total_frames as f64 / pipeline_elapsed.as_secs_f64()
    );

    Ok(())
}

/// Spawn a single worker process for a segment and collect its result.
async fn spawn_worker(
    worker_bin: &Path,
    worker_id: usize,
    desc: &SegmentDescriptor,
    input_path: &Path,
    output_dir: &Path,
    crf: u32,
    preset: &str,
    encoder: &str,
    verbose: bool,
    presplit_path: Option<&Path>,
    hw_decode: bool,
    multi_progress: &MultiProgress,
) -> Result<WorkerResult> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let segment_file = output_dir.join(format!("segment_{:04}.ts", desc.id));
    let segment_json =
        serde_json::to_string(desc).context("Failed to serialize segment descriptor")?;

    // When pre-split, use the pre-split file as input instead of the full video
    let worker_input = presplit_path.unwrap_or(input_path);

    let mut cmd = Command::new(worker_bin);
    cmd.arg("--input")
        .arg(worker_input)
        .arg("--output")
        .arg(&segment_file)
        .arg("--worker-id")
        .arg(worker_id.to_string())
        .arg("--segment")
        .arg(&segment_json)
        .arg("--crf")
        .arg(crf.to_string())
        .arg("--preset")
        .arg(preset)
        .arg("--encoder")
        .arg(encoder);

    if presplit_path.is_some() {
        cmd.arg("--presplit");
    }

    if hw_decode {
        cmd.arg("--hw-decode");
    }

    if verbose {
        cmd.arg("--verbose");
    }

    // Worker logs to stderr, result JSON to stdout
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let total_frames = desc.end_frame.saturating_sub(desc.start_frame) + 1;
    let seg_pb = multi_progress.add(ProgressBar::new(total_frames));
    seg_pb.set_style(
        ProgressStyle::default_bar()
            .template("  [Worker {prefix}] {bar:30.green/black} {percent:>3}% • {pos}/{len} frames • ETA {eta_precise}")
            .unwrap()
            .progress_chars("##-"),
    );
    seg_pb.set_prefix(worker_id.to_string());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn worker for segment {}", desc.id))?;

    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    let seg_pb_stderr = seg_pb.clone();
    let stderr_task = tokio::spawn(async move {
        let mut captured = String::new();
        if let Some(stderr) = stderr_handle {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                if let Some(rest) = line.split("Encoded ").nth(1) {
                    if let Some(n_str) = rest.split_whitespace().next() {
                        if let Ok(n) = n_str.parse::<u64>() {
                            seg_pb_stderr.set_position(n.min(total_frames));
                        }
                    }
                }
                captured.push_str(&line);
                captured.push('\n');
            }
        }
        captured
    });

    let stdout_task = tokio::spawn(async move {
        let mut captured = String::new();
        if let Some(stdout) = stdout_handle {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                captured.push_str(&line);
                captured.push('\n');
            }
        }
        captured
    });

    let status = child
        .wait()
        .await
        .with_context(|| format!("Failed to wait for worker for segment {}", desc.id))?;

    let stderr_output = stderr_task.await.unwrap_or_default();
    let stdout_output = stdout_task.await.unwrap_or_default();

    seg_pb.finish_and_clear();

    if !status.success() {
        anyhow::bail!(
            "Worker for segment {} exited with {}: {}",
            desc.id,
            status,
            stderr_output.lines().last().unwrap_or("(no output)")
        );
    }

    // The worker prints the JSON result as the last line of stdout
    let json_line = stdout_output
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with('{'))
        .ok_or_else(|| anyhow::anyhow!("No JSON result found in worker stdout for segment {}", desc.id))?;

    let result: WorkerResult = serde_json::from_str(json_line)
        .with_context(|| format!("Failed to parse worker result JSON for segment {}", desc.id))?;

    Ok(result)
}

/// Generate HLS playlist(s) from completed segment results.
///
/// When audio or subtitle tracks exist, generates:
/// - `playlist.m3u8` (master): EXT-X-MEDIA tags + EXT-X-STREAM-INF
/// - `video.m3u8` (media): segment listing
///
/// When no extra tracks exist, generates a single `playlist.m3u8` (backward compatible).
fn generate_hls_playlist(
    playlist_path: &Path,
    results: &[WorkerResult],
    segments: &[segmenter::Segment],
    _fps: f64,
    audio_tracks: &[media::AudioTrackInfo],
    subtitle_vtt_files: &[(String, PathBuf)],
) -> Result<()> {
    use std::io::Write;

    let max_duration = segments
        .iter()
        .map(|s| s.end_timestamp - s.start_timestamp)
        .fold(0.0f64, f64::max)
        .ceil() as u64;

    let has_extra_tracks = !audio_tracks.is_empty() || !subtitle_vtt_files.is_empty();

    if has_extra_tracks {
        // Master playlist
        let master_path = playlist_path;
        let media_playlist_name = "video.m3u8";
        let media_path = playlist_path.parent().unwrap().join(media_playlist_name);

        let mut master = std::fs::File::create(master_path)
            .with_context(|| format!("Failed to create master playlist: {}", master_path.display()))?;

        writeln!(master, "#EXTM3U")?;

        // Audio renditions
        for track in audio_tracks {
            let lang = track.language.as_deref().unwrap_or("und");
            let name = track.language.as_deref().unwrap_or("Default");
            let default = if track.is_default { "YES" } else { "NO" };
            writeln!(master, "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",LANGUAGE=\"{}\",NAME=\"{}\",DEFAULT={},AUTOSELECT=YES",
                lang, name, default)?;
        }

        // Subtitle renditions
        for (lang, vtt_path) in subtitle_vtt_files {
            let filename = vtt_path.file_name().unwrap_or_default().to_string_lossy();
            writeln!(master, "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",LANGUAGE=\"{}\",NAME=\"{}\",DEFAULT=NO,AUTOSELECT=YES,URI=\"{}\"",
                lang, lang, filename)?;
        }

        // Stream inf
        let mut stream_inf = "#EXT-X-STREAM-INF:BANDWIDTH=0".to_string();
        if !audio_tracks.is_empty() {
            stream_inf.push_str(",AUDIO=\"audio\"");
        }
        if !subtitle_vtt_files.is_empty() {
            stream_inf.push_str(",SUBTITLES=\"subs\"");
        }
        writeln!(master, "{}", stream_inf)?;
        writeln!(master, "{}", media_playlist_name)?;

        // Media playlist (video segments)
        let mut media = std::fs::File::create(&media_path)
            .with_context(|| format!("Failed to create media playlist: {}", media_path.display()))?;

        writeln!(media, "#EXTM3U")?;
        writeln!(media, "#EXT-X-VERSION:3")?;
        writeln!(media, "#EXT-X-TARGETDURATION:{}", max_duration)?;
        writeln!(media, "#EXT-X-MEDIA-SEQUENCE:0")?;

        for result in results {
            let seg = segments.iter().find(|s| s.id == result.segment_id);
            let duration = seg.map(|s| s.end_timestamp - s.start_timestamp).unwrap_or(0.0);
            let filename = Path::new(&result.output_path).file_name().unwrap_or_default().to_string_lossy();
            writeln!(media, "#EXTINF:{:.6},", duration)?;
            writeln!(media, "{}", filename)?;
        }
        writeln!(media, "#EXT-X-ENDLIST")?;

        info!("  HLS master playlist: {}", master_path.display());
        info!("  HLS media playlist: {}", media_path.display());
    } else {
        // Single playlist (backward compatible)
        let mut f = std::fs::File::create(playlist_path)
            .with_context(|| format!("Failed to create playlist: {}", playlist_path.display()))?;

        writeln!(f, "#EXTM3U")?;
        writeln!(f, "#EXT-X-VERSION:3")?;
        writeln!(f, "#EXT-X-TARGETDURATION:{}", max_duration)?;
        writeln!(f, "#EXT-X-MEDIA-SEQUENCE:0")?;

        for result in results {
            let seg = segments.iter().find(|s| s.id == result.segment_id);
            let duration = seg.map(|s| s.end_timestamp - s.start_timestamp).unwrap_or(0.0);
            let filename = Path::new(&result.output_path).file_name().unwrap_or_default().to_string_lossy();
            writeln!(f, "#EXTINF:{:.6},", duration)?;
            writeln!(f, "{}", filename)?;
        }

        writeln!(f, "#EXT-X-ENDLIST")?;
    }

    Ok(())
}

/// Concatenate .ts segments into a single MP4 using ffmpeg, with optional chapter metadata
/// and audio muxing from the original source file.
fn concatenate_mp4(
    output_path: &Path,
    results: &[WorkerResult],
    chapters: &[media::ChapterInfo],
    audio_source: Option<&str>,
) -> Result<()> {
    use std::io::Write;

    let concat_list = output_path.with_extension("concat.txt");
    {
        let mut f = std::fs::File::create(&concat_list)?;
        for result in results {
            let filename = Path::new(&result.output_path)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            writeln!(f, "file '{}'", filename)?;
        }
    }

    let mut cmd = std::process::Command::new("ffmpeg");
    cmd.arg("-y")
        .arg("-f").arg("concat")
        .arg("-safe").arg("0")
        .arg("-i").arg(&concat_list);

    // Input index tracking: 0 = concat video
    let mut next_input_idx = 1u32;

    // Add audio source as a separate input if provided
    let audio_input_idx = if let Some(src) = audio_source {
        let idx = next_input_idx;
        next_input_idx += 1;
        cmd.arg("-i").arg(src);
        Some(idx)
    } else {
        None
    };

    // If chapters exist, write FFMETADATA1 and add as input
    let metadata_path = output_path.with_extension("metadata.txt");
    let metadata_input_idx = if !chapters.is_empty() {
        let mut meta_file = std::fs::File::create(&metadata_path)?;
        writeln!(meta_file, ";FFMETADATA1")?;
        for ch in chapters {
            writeln!(meta_file, "[CHAPTER]")?;
            writeln!(meta_file, "TIMEBASE=1/1000")?;
            writeln!(meta_file, "START={}", (ch.start_secs * 1000.0) as i64)?;
            writeln!(meta_file, "END={}", (ch.end_secs * 1000.0) as i64)?;
            if let Some(ref title) = ch.title {
                writeln!(meta_file, "title={}", title)?;
            }
        }
        let idx = next_input_idx;
        cmd.arg("-i").arg(&metadata_path);
        Some(idx)
    } else {
        None
    };

    // Map video from concat input (input 0)
    cmd.arg("-map").arg("0:v");

    // Map audio from original source if available
    if let Some(idx) = audio_input_idx {
        cmd.arg("-map").arg(format!("{}:a?", idx));
    }

    // Map chapter metadata if present
    if let Some(idx) = metadata_input_idx {
        cmd.arg("-map_metadata").arg(format!("{}", idx));
    }

    // Copy video stream, copy audio stream (already encoded in source)
    cmd.arg("-c:v").arg("copy");
    if audio_input_idx.is_some() {
        cmd.arg("-c:a").arg("copy")
            .arg("-shortest");
    }

    cmd.arg(output_path);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    info!("Running ffmpeg MP4 mux: {:?}", cmd);

    let output = cmd.output().context("Failed to run ffmpeg for MP4 concatenation")?;

    let _ = std::fs::remove_file(&concat_list);
    let _ = std::fs::remove_file(&metadata_path);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffmpeg concat failed with exit code {}: {}", output.status, stderr);
    }

    Ok(())
}

/// Pre-split the input video into per-segment files using ffmpeg stream copy.
///
/// Each segment is extracted with `-ss start -to end -c copy` which is nearly instant
/// since it only copies packets without re-encoding. Workers then read only their
/// small pre-split file instead of seeking through the full input.
async fn presplit_segments(
    input_path: &str,
    segments: &[segmenter::Segment],
    output_dir: &Path,
    final_output: bool,
    progress: Option<&ProgressBar>,
    has_text_subs: bool,
) -> Result<Vec<PathBuf>> {
    let tasks: Vec<_> = segments
        .iter()
        .map(|seg| {
            let input = input_path.to_string();
            let filename = if final_output {
                format!("segment_{:04}.ts", seg.id)
            } else {
                format!("presplit_{:04}.ts", seg.id)
            };
            let out_path = output_dir.join(filename);
            let start = seg.start_timestamp;
            let end = seg.end_timestamp;
            let id = seg.id;
            let exclude_text_subs = has_text_subs;
            async move {
                let mut ffmpeg_args = vec![
                    "-y".to_string(),
                    "-ss".to_string(), format!("{:.6}", start),
                    "-to".to_string(), format!("{:.6}", end),
                    "-i".to_string(), input.clone(),
                    "-map".to_string(), "0".to_string(),
                ];
                if exclude_text_subs {
                    ffmpeg_args.extend(["-map".to_string(), "-0:s".to_string()]);
                }
                ffmpeg_args.extend([
                    "-c".to_string(), "copy".to_string(),
                    "-avoid_negative_ts".to_string(), "make_zero".to_string(),
                    out_path.to_str().unwrap().to_string(),
                ]);

                let output = Command::new("ffmpeg")
                    .args(&ffmpeg_args)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::piped())
                    .output()
                    .await
                    .with_context(|| format!("Failed to pre-split segment {}", id))?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("ffmpeg pre-split segment {} failed: {}", id, stderr);
                }

                debug!("Pre-split segment {} -> {}", id, out_path.display());
                Ok::<PathBuf, anyhow::Error>(out_path)
            }
        })
        .collect();

    // Run all pre-splits in parallel with bounded concurrency
    let pb = progress.cloned();
    let results: Vec<Result<PathBuf>> = stream::iter(tasks)
        .buffer_unordered(8) // up to 8 concurrent ffmpeg processes
        .inspect(|_| {
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
        })
        .collect()
        .await;

    let mut paths = Vec::with_capacity(results.len());
    for result in results {
        paths.push(result?);
    }

    // Sort by segment id (filename contains the id)
    paths.sort();
    Ok(paths)
}

/// Extract text-based subtitle tracks as WebVTT files for HLS.
async fn extract_subtitles_as_webvtt(
    input_path: &str,
    subtitle_tracks: &[media::SubtitleTrackInfo],
    output_dir: &Path,
) -> Result<Vec<(String, PathBuf)>> {
    let mut results = Vec::new();

    for track in subtitle_tracks {
        if !track.is_text_based {
            continue;
        }

        let lang = track.language.as_deref().unwrap_or("und");
        let filename = format!("subs_{}.vtt", lang);
        let out_path = output_dir.join(&filename);

        let output = Command::new("ffmpeg")
            .args([
                "-y",
                "-i", input_path,
                "-map", &format!("0:{}", track.stream_index),
                "-c:s", "webvtt",
                out_path.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .with_context(|| format!("Failed to extract subtitle track {}", track.stream_index))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Subtitle extraction for track {} failed: {}", track.stream_index, stderr);
            continue;
        }

        info!("Extracted subtitle track {} [{}] -> {}", track.stream_index, lang, filename);
        results.push((lang.to_string(), out_path));
    }

    Ok(results)
}

/// Find the worker binary. Checks:
/// 1. Same directory as the coordinator binary
/// 2. cargo target directory (for development)
/// 3. PATH
fn find_worker_binary() -> Result<PathBuf> {
    let worker_name = "transcoder-worker";

    // Check same directory as current executable
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.parent().unwrap().join(worker_name);
        if sibling.exists() {
            return Ok(sibling);
        }
    }

    // Check if it's in PATH (cargo run scenario — both binaries get built to target/)
    if let Ok(output) = std::process::Command::new("which")
        .arg(worker_name)
        .output()
    {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return Ok(PathBuf::from(path));
            }
        }
    }

    // Last resort: assume cargo workspace — look in target/debug or target/release
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace_root = Path::new(manifest_dir).parent().unwrap();
    for profile in &["debug", "release"] {
        let candidate = workspace_root.join("target").join(profile).join(worker_name);
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    anyhow::bail!(
        "Could not find '{}' binary. Build the workspace first with `cargo build`.",
        worker_name
    )
}

/// Submit a transcoding job to the cluster master and monitor progress.
async fn run_cluster_mode(
    args: &Args,
    input_path: &Path,
    _metadata: &media::VideoMetadata,
    _segments: &[segmenter::Segment],
) -> Result<()> {
    use transcoder_cluster::protocol::*;

    let master_addr = args.cluster_master.as_deref()
        .unwrap_or("127.0.0.1:9900");

    info!("Cluster mode: connecting to master at {}", master_addr);

    // Connect via WebSocket
    let ws_url = format!("ws://{}", master_addr);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await
        .context("Failed to connect to cluster master")?;

    use futures::SinkExt;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite;

    // Submit job
    let job_id = uuid::Uuid::new_v4();
    let input_size = std::fs::metadata(input_path)?.len();

    let submit = JobSubmitData {
        job_id,
        input_filename: input_path.to_string_lossy().to_string(),
        input_size_bytes: input_size,
        config: EncodingConfig {
            crf: args.crf,
            preset: args.preset.clone(),
            encoder: args.encoder.clone(),
            format: args.format.clone(),
            fast_mode: args.fast,
            hw_decode: false,
        },
        srt_input_url: None,
    };

    let msg = Message::new(OpCode::JobSubmit, &submit)?;
    let json = serde_json::to_string(&msg)?;
    ws_stream.send(tungstenite::Message::Text(json.into())).await?;

    info!("Job {} submitted to cluster, waiting for completion...", job_id);

    // Wait for responses
    while let Some(result) = ws_stream.next().await {
        match result {
            Ok(tungstenite::Message::Text(text)) => {
                if let Ok(msg) = serde_json::from_str::<Message>(&text) {
                    match msg.op {
                        OpCode::JobAccepted => {
                            if let Ok(data) = msg.parse_data::<JobAcceptedData>() {
                                info!("Job accepted: {} segments to process", data.total_segments);
                            }
                        }
                        OpCode::JobProgress => {
                            if let Ok(data) = msg.parse_data::<JobProgressData>() {
                                info!(
                                    "Progress: {}/{} segments ({} failed) — {}",
                                    data.completed_segments, data.total_segments,
                                    data.failed_segments, data.phase
                                );
                            }
                        }
                        OpCode::JobComplete => {
                            info!("Job completed successfully!");
                            break;
                        }
                        OpCode::JobFailed => {
                            let err_msg = msg.d.get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown error");
                            anyhow::bail!("Job failed: {}", err_msg);
                        }
                        OpCode::Error => {
                            if let Ok(data) = msg.parse_data::<ErrorData>() {
                                anyhow::bail!("Cluster error: {}", data.message);
                            }
                        }
                        _ => {
                            debug!("Received {:?}", msg.op);
                        }
                    }
                }
            }
            Ok(tungstenite::Message::Close(_)) => {
                anyhow::bail!("Connection to cluster master closed unexpectedly");
            }
            Err(e) => {
                anyhow::bail!("WebSocket error: {}", e);
            }
            _ => {}
        }
    }

    Ok(())
}
