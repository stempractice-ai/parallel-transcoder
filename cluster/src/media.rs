//! Source analysis and the copy/encode rules shared by the coordinator and
//! the cluster master. Uses ffprobe as a subprocess only, so this module needs
//! no libav and the crate still builds without ffmpeg.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command as StdCommand;
use tracing::{info, warn};

/// Per-GOP statistics computed from packet data (no decoding required).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GopStats {
    pub start_frame: u64,
    pub end_frame: u64,
    pub start_time: f64,
    pub end_time: f64,
    pub size_bytes: u64,
    pub bitrate_bps: f64,
}

/// Audio track information extracted from ffprobe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioTrackInfo {
    pub stream_index: usize,
    pub codec_name: String,
    pub language: Option<String>,
    pub channels: u32,
    pub channel_layout: Option<String>,
    pub sample_rate: u32,
    pub bitrate_bps: Option<u64>,
    pub is_default: bool,
}

/// Subtitle track information extracted from ffprobe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleTrackInfo {
    pub stream_index: usize,
    pub codec_name: String,
    pub language: Option<String>,
    pub is_text_based: bool,
    pub is_default: bool,
}

/// Chapter information extracted from ffprobe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChapterInfo {
    pub id: u64,
    pub start_secs: f64,
    pub end_secs: f64,
    pub title: Option<String>,
}

/// Video metadata extracted from analysis
#[derive(Debug, Clone)]
pub struct VideoMetadata {
    pub duration_secs: f64,
    pub width: u32,
    pub height: u32,
    pub fps: f64,
    pub total_frames: u64,
    pub codec_name: String,
    pub keyframe_positions: Vec<u64>,
    pub keyframe_timestamps: Vec<f64>,
    pub scene_changes: Vec<u64>,
    pub complexity_map: Vec<f32>,
    /// Per-GOP bitrate stats (only populated by fast_analyze_video)
    pub gop_stats: Vec<GopStats>,
    pub profile: Option<String>,
    pub pix_fmt: Option<String>,
    pub audio_tracks: Vec<AudioTrackInfo>,
    pub subtitle_tracks: Vec<SubtitleTrackInfo>,
    pub chapters: Vec<ChapterInfo>,
}

/// Fast video analysis using ffprobe subprocesses instead of full decode.
///
/// Runs two ffprobe commands:
/// 1. Stream/format metadata (fps, resolution, duration, codec)
/// 2. Packet-level keyframe positions (flags with 'K')
///
/// Returns VideoMetadata with real keyframes but uniform complexity and no scene changes.
/// Drops analysis time from ~68s to <1s.
pub fn fast_analyze_video(input_path: &str) -> Result<VideoMetadata> {
    let path = Path::new(input_path);
    if !path.exists() {
        anyhow::bail!("Input file does not exist: {}", input_path);
    }

    info!("Fast analysis via ffprobe: {}", input_path);

    // 1. Get stream and format metadata
    let probe_output = StdCommand::new("ffprobe")
        .args([
            "-v", "quiet",
            "-show_streams",
            "-show_format",
            "-show_chapters",
            "-print_format", "json",
            input_path,
        ])
        .output()
        .context("Failed to run ffprobe for metadata")?;

    if !probe_output.status.success() {
        let stderr = String::from_utf8_lossy(&probe_output.stderr);
        anyhow::bail!("ffprobe metadata failed: {}", stderr);
    }

    let probe_json: serde_json::Value =
        serde_json::from_slice(&probe_output.stdout).context("Failed to parse ffprobe JSON")?;

    // Extract video stream info
    let streams = probe_json["streams"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("No streams found in ffprobe output"))?;

    let stream = streams
        .iter()
        .find(|s| s["codec_type"].as_str() == Some("video"))
        .ok_or_else(|| anyhow::anyhow!("No video stream found in ffprobe output"))?;

    let width = stream["width"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("Missing width"))? as u32;
    let height = stream["height"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("Missing height"))? as u32;
    let codec_name = stream["codec_name"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let profile = stream["profile"].as_str().map(|s| s.to_string());
    let pix_fmt = stream["pix_fmt"].as_str().map(|s| s.to_string());

    // Parse fps from r_frame_rate (e.g. "30000/1001")
    let fps = parse_rational_str(stream["r_frame_rate"].as_str().unwrap_or("30/1"));

    // Duration: try stream duration, then format duration
    let duration_secs = stream["duration"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| {
            probe_json["format"]["duration"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
        })
        .unwrap_or(0.0);

    // Total frames: try nb_frames, then estimate from duration * fps
    let total_frames = stream["nb_frames"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| (duration_secs * fps).round() as u64);

    // Parse audio tracks
    let audio_tracks: Vec<AudioTrackInfo> = streams
        .iter()
        .filter(|s| s["codec_type"].as_str() == Some("audio"))
        .map(|s| {
            let tags = &s["tags"];
            AudioTrackInfo {
                stream_index: s["index"].as_u64().unwrap_or(0) as usize,
                codec_name: s["codec_name"].as_str().unwrap_or("unknown").to_string(),
                language: tags["language"].as_str().map(|l| l.to_string()),
                channels: s["channels"].as_u64().unwrap_or(0) as u32,
                channel_layout: s["channel_layout"].as_str().map(|l| l.to_string()),
                sample_rate: s["sample_rate"]
                    .as_str()
                    .and_then(|sr| sr.parse().ok())
                    .unwrap_or(0),
                bitrate_bps: s["bit_rate"]
                    .as_str()
                    .and_then(|br| br.parse().ok()),
                is_default: s["disposition"]["default"].as_u64() == Some(1),
            }
        })
        .collect();

    // Parse subtitle tracks
    let subtitle_tracks: Vec<SubtitleTrackInfo> = streams
        .iter()
        .filter(|s| s["codec_type"].as_str() == Some("subtitle"))
        .map(|s| {
            let codec = s["codec_name"].as_str().unwrap_or("unknown");
            let tags = &s["tags"];
            SubtitleTrackInfo {
                stream_index: s["index"].as_u64().unwrap_or(0) as usize,
                codec_name: codec.to_string(),
                language: tags["language"].as_str().map(|l| l.to_string()),
                is_text_based: matches!(codec, "srt" | "ass" | "ssa" | "webvtt" | "subrip" | "mov_text" | "text"),
                is_default: s["disposition"]["default"].as_u64() == Some(1),
            }
        })
        .collect();

    // Parse chapters
    let chapters: Vec<ChapterInfo> = probe_json["chapters"]
        .as_array()
        .map(|chs| {
            chs.iter()
                .map(|ch| {
                    let time_base_str = ch["time_base"].as_str().unwrap_or("1/1000000000");
                    let tb = parse_rational_str(time_base_str);
                    ChapterInfo {
                        id: ch["id"].as_u64().unwrap_or(0),
                        start_secs: ch["start"].as_i64().unwrap_or(0) as f64 * tb,
                        end_secs: ch["end"].as_i64().unwrap_or(0) as f64 * tb,
                        title: ch["tags"]["title"].as_str().map(|t| t.to_string()),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    info!(
        "Fast probe: {}x{} @ {:.2} fps, {:.2}s, {} frames, codec={}",
        width, height, fps, duration_secs, total_frames, codec_name
    );

    // 2. Get keyframe positions and packet sizes via packet probing
    let packets_output = StdCommand::new("ffprobe")
        .args([
            "-v", "quiet",
            "-show_packets",
            "-show_entries", "packet=pts_time,flags,size",
            "-select_streams", "v:0",
            "-print_format", "json",
            input_path,
        ])
        .output()
        .context("Failed to run ffprobe for keyframes")?;

    if !packets_output.status.success() {
        let stderr = String::from_utf8_lossy(&packets_output.stderr);
        anyhow::bail!("ffprobe packets failed: {}", stderr);
    }

    let packets_json: serde_json::Value = serde_json::from_slice(&packets_output.stdout)
        .context("Failed to parse ffprobe packets JSON")?;

    let mut keyframe_positions: Vec<u64> = Vec::new();
    let mut keyframe_timestamps: Vec<f64> = Vec::new();

    // Collect per-packet data for GOP bitrate computation
    struct PacketInfo {
        pts_time: f64,
        size: u64,
        is_key: bool,
    }
    let mut all_packets: Vec<PacketInfo> = Vec::new();

    if let Some(packets) = packets_json["packets"].as_array() {
        let mut frame_index: u64 = 0;
        for pkt in packets {
            let flags = pkt["flags"].as_str().unwrap_or("");
            let pts_time = pkt["pts_time"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let size = pkt["size"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let is_key = flags.contains('K');

            if is_key {
                keyframe_positions.push(frame_index);
                keyframe_timestamps.push(pts_time);
            }

            all_packets.push(PacketInfo { pts_time, size, is_key });
            frame_index += 1;
        }
    }

    // Ensure frame 0 is always a keyframe
    if keyframe_positions.is_empty() || keyframe_positions[0] != 0 {
        warn!("No keyframe at frame 0 detected, inserting one");
        keyframe_positions.insert(0, 0);
        keyframe_timestamps.insert(0, 0.0);
    }

    // Compute per-GOP bitrate stats
    let mut gop_stats: Vec<GopStats> = Vec::new();
    let mut gop_start_frame: u64 = 0;
    let mut gop_start_time: f64 = 0.0;
    let mut gop_size: u64 = 0;

    for (i, pkt) in all_packets.iter().enumerate() {
        if pkt.is_key && i > 0 {
            // Close previous GOP
            let gop_end_frame = i as u64 - 1;
            let gop_end_time = all_packets[i - 1].pts_time;
            let gop_duration = gop_end_time - gop_start_time;
            let bitrate = if gop_duration > 0.001 {
                gop_size as f64 * 8.0 / gop_duration
            } else {
                0.0
            };
            gop_stats.push(GopStats {
                start_frame: gop_start_frame,
                end_frame: gop_end_frame,
                start_time: gop_start_time,
                end_time: gop_end_time,
                size_bytes: gop_size,
                bitrate_bps: bitrate,
            });

            // Start new GOP
            gop_start_frame = i as u64;
            gop_start_time = pkt.pts_time;
            gop_size = 0;
        }
        gop_size += pkt.size;
    }

    // Close final GOP
    if !all_packets.is_empty() {
        let last = all_packets.len() - 1;
        let gop_end_time = all_packets[last].pts_time;
        let gop_duration = gop_end_time - gop_start_time;
        let bitrate = if gop_duration > 0.001 {
            gop_size as f64 * 8.0 / gop_duration
        } else {
            0.0
        };
        gop_stats.push(GopStats {
            start_frame: gop_start_frame,
            end_frame: last as u64,
            start_time: gop_start_time,
            end_time: gop_end_time,
            size_bytes: gop_size,
            bitrate_bps: bitrate,
        });
    }

    // Log GOP bitrate stats
    let avg_bitrate: f64 = if !gop_stats.is_empty() {
        gop_stats.iter().map(|g| g.bitrate_bps).sum::<f64>() / gop_stats.len() as f64
    } else {
        0.0
    };
    let min_bitrate = gop_stats.iter().map(|g| g.bitrate_bps).fold(f64::MAX, f64::min);
    let max_bitrate = gop_stats.iter().map(|g| g.bitrate_bps).fold(0.0f64, f64::max);

    info!(
        "Fast analysis complete: {} keyframes, {} GOPs in {:.2}s video",
        keyframe_positions.len(),
        gop_stats.len(),
        duration_secs
    );
    info!(
        "  GOP bitrates: avg={:.1} Mbps, min={:.1} Mbps, max={:.1} Mbps",
        avg_bitrate / 1_000_000.0,
        min_bitrate / 1_000_000.0,
        max_bitrate / 1_000_000.0
    );
    if !audio_tracks.is_empty() {
        info!("  Audio tracks: {}", audio_tracks.len());
        for t in &audio_tracks {
            info!("    #{}: {} {}ch{}", t.stream_index, t.codec_name, t.channels,
                t.language.as_deref().map(|l| format!(" [{}]", l)).unwrap_or_default());
        }
    }
    if !subtitle_tracks.is_empty() {
        info!("  Subtitle tracks: {}", subtitle_tracks.len());
        for t in &subtitle_tracks {
            info!("    #{}: {}{}{}", t.stream_index, t.codec_name,
                if t.is_text_based { " (text)" } else { " (bitmap)" },
                t.language.as_deref().map(|l| format!(" [{}]", l)).unwrap_or_default());
        }
    }
    if !chapters.is_empty() {
        info!("  Chapters: {}", chapters.len());
    }

    // Return metadata with uniform complexity and no scene changes
    let num_frames = total_frames as usize;
    Ok(VideoMetadata {
        duration_secs,
        width,
        height,
        fps,
        total_frames,
        codec_name,
        keyframe_positions,
        keyframe_timestamps,
        scene_changes: vec![],
        complexity_map: vec![0.5; num_frames],
        gop_stats,
        profile,
        pix_fmt,
        audio_tracks,
        subtitle_tracks,
        chapters,
    })
}

/// Parse a rational string like "30000/1001" into a f64.
fn parse_rational_str(s: &str) -> f64 {
    if let Some((num, den)) = s.split_once('/') {
        let n: f64 = num.parse().unwrap_or(30.0);
        let d: f64 = den.parse().unwrap_or(1.0);
        if d != 0.0 { n / d } else { 30.0 }
    } else {
        s.parse().unwrap_or(30.0)
    }
}

/// Reasons a segment cannot be stream-copied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CopyBlocker {
    CodecMismatch { source: String, target: String },
    ProfileIncompatible { source: String, target: String },
    PixFmtMismatch { source: String, target: String },
}

/// Map encoder name to the codec name that ffprobe reports.
pub fn encoder_to_codec(encoder: &str) -> &str {
    match encoder {
        "libx264" | "h264_videotoolbox" | "videotoolbox" | "h264_nvenc" | "h264_vaapi" => "h264",
        "libx265" | "hevc_videotoolbox" | "hevc_nvenc" | "hevc_vaapi" => "hevc",
        "libvpx-vp9" => "vp9",
        "libaom-av1" | "libsvtav1" | "av1_nvenc" => "av1",
        other => other,
    }
}

/// Estimate the target bitrate (bps) for a given resolution and CRF.
///
/// These are empirical estimates for typical video content at CRF 23.
/// Used by smart mode to decide if source GOPs are close enough to skip re-encoding.
pub fn estimate_target_bitrate(width: u32, height: u32, crf: u32, encoder: &str) -> f64 {
    let pixels = width as f64 * height as f64;

    // Base bitrate estimates for CRF 23 at common resolutions
    let base_bitrate = if pixels >= 3840.0 * 2160.0 {
        40_000_000.0  // 4K: ~40 Mbps
    } else if pixels >= 1920.0 * 1080.0 {
        8_000_000.0   // 1080p: ~8 Mbps
    } else if pixels >= 1280.0 * 720.0 {
        4_000_000.0   // 720p: ~4 Mbps
    } else {
        2_000_000.0   // SD: ~2 Mbps
    };

    // CRF adjustment: each CRF unit roughly corresponds to ~12% bitrate change
    // CRF 23 is our baseline
    let crf_factor = 1.12f64.powi(23i32 - crf as i32);

    // Encoder efficiency factors relative to libx264 baseline:
    // - H.265/HEVC achieves ~50% bitrate at equivalent quality
    // - AV1 achieves ~30-40% bitrate at equivalent quality
    // - VideoToolbox produces higher bitrate than software encoders
    let encoder_factor = match encoder {
        e if e.contains("videotoolbox") && e.contains("hevc") => 1.0,
        e if e.contains("videotoolbox") => 2.0,
        e if e.contains("nvenc") && (e.contains("hevc") || e.contains("av1")) => 0.7,
        e if e.contains("nvenc") => 1.5,
        e if e.contains("vaapi") && e.contains("hevc") => 0.8,
        e if e.contains("vaapi") => 1.5,
        "libx265" | "hevc_nvenc" | "hevc_vaapi" => 0.5,
        "libsvtav1" => 0.4,
        "libaom-av1" => 0.35,
        "av1_nvenc" => 0.5,
        _ => 1.0,
    };

    base_bitrate * crf_factor * encoder_factor
}

/// Check global copy-compatibility between source and target encoding settings.
pub fn check_copy_compatibility(
    metadata: &VideoMetadata,
    target_codec: &str,
    encoder: &str,
) -> Vec<CopyBlocker> {
    let mut blockers = Vec::new();

    // Codec check
    if target_codec != metadata.codec_name {
        blockers.push(CopyBlocker::CodecMismatch {
            source: metadata.codec_name.clone(),
            target: target_codec.to_string(),
        });
    }

    // Profile compatibility (only for H.264)
    if target_codec == "h264" && metadata.profile.is_some() {
        let source_profile = metadata.profile.as_deref().unwrap();
        let target_profile = encoder_target_profile(encoder);
        if !is_profile_compatible(source_profile, target_profile) {
            blockers.push(CopyBlocker::ProfileIncompatible {
                source: source_profile.to_string(),
                target: target_profile.to_string(),
            });
        }
    }

    // Pixel format check
    if let Some(ref src_pix_fmt) = metadata.pix_fmt {
        let target_pix = encoder_target_pix_fmt(encoder);
        if src_pix_fmt != target_pix {
            blockers.push(CopyBlocker::PixFmtMismatch {
                source: src_pix_fmt.clone(),
                target: target_pix.to_string(),
            });
        }
    }

    blockers
}

/// Rank H.264 profiles: Baseline < Main < High
fn profile_rank(profile: &str) -> u32 {
    match profile.to_lowercase().as_str() {
        "baseline" | "constrained baseline" => 1,
        "main" => 2,
        "high" | "high 10" | "high 4:2:2" | "high 4:4:4" | "high 4:4:4 predictive" => 3,
        _ => 2, // default to Main
    }
}

/// Check if source profile is compatible with target (source rank <= target rank).
fn is_profile_compatible(source: &str, target: &str) -> bool {
    profile_rank(source) <= profile_rank(target)
}

/// Default target profile for an encoder.
fn encoder_target_profile(encoder: &str) -> &str {
    match encoder {
        "h264_videotoolbox" | "videotoolbox" => "Main",
        "libx264" => "High",
        _ => "High",
    }
}

/// Default target pixel format for an encoder.
fn encoder_target_pix_fmt(encoder: &str) -> &str {
    match encoder {
        "libx264" | "h264_videotoolbox" | "videotoolbox" => "yuv420p",
        "libx265" | "hevc_videotoolbox" => "yuv420p",
        _ => "yuv420p",
    }
}

/// Auto-tune smart tolerance from GOP bitrate distribution.
pub fn auto_tune_tolerance(gop_stats: &[GopStats], target_bitrate: f64, max_tolerance: f64) -> f64 {
    if gop_stats.is_empty() || target_bitrate <= 0.0 {
        return max_tolerance;
    }

    let ratios: Vec<f64> = gop_stats.iter().map(|g| g.bitrate_bps / target_bitrate).collect();
    let n = ratios.len() as f64;
    let mean = ratios.iter().sum::<f64>() / n;
    let variance = ratios.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    let stddev = variance.sqrt();

    // Tight cluster near target = tight tolerance; spread out = wider tolerance
    let tolerance = (2.0 * stddev - (mean - 1.0).abs()).clamp(0.05, max_tolerance);

    tolerance
}

/// Whole-file facts the UI shows next to a recommendation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MediaSummary {
    pub duration_secs: f64,
    pub width: u32,
    pub height: u32,
    pub fps: f64,
    pub codec: String,
    pub profile: Option<String>,
    pub pix_fmt: Option<String>,
    /// Mean GOP bitrate; None when the probe found no GOPs.
    pub avg_bitrate_bps: Option<f64>,
    pub has_audio: bool,
}

impl From<&VideoMetadata> for MediaSummary {
    fn from(meta: &VideoMetadata) -> Self {
        let avg_bitrate_bps = if meta.gop_stats.is_empty() {
            None
        } else {
            Some(meta.gop_stats.iter().map(|g| g.bitrate_bps).sum::<f64>() / meta.gop_stats.len() as f64)
        };
        MediaSummary {
            duration_secs: meta.duration_secs,
            width: meta.width,
            height: meta.height,
            fps: meta.fps,
            codec: meta.codec_name.clone(),
            profile: meta.profile.clone(),
            pix_fmt: meta.pix_fmt.clone(),
            avg_bitrate_bps,
            has_audio: !meta.audio_tracks.is_empty(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Recommendation {
    /// One of "normal", "copy", "smart-auto".
    pub mode: String,
    /// An x264 preset name: "slow", "medium", "fast" or "veryfast".
    pub preset: String,
    pub reasons: Vec<String>,
    /// Fraction of GOPs whose bitrate is within tolerance of the target; None without GOP data.
    pub copyable_gop_fraction: Option<f64>,
}

/// Per-segment copy/encode plan shared by the coordinator and the cluster master.
pub struct SegmentPlan {
    pub copy: Vec<bool>,
    pub bitrate_ratio: Vec<Option<f64>>,
    pub blockers: Vec<CopyBlocker>,
    pub tolerance: f64,
    pub target_bitrate: f64,
}

/// At least this fraction of on-target GOPs recommends stream-copying everything.
pub const COPY_FRACTION: f64 = 0.9;
/// At most this fraction of on-target GOPs recommends re-encoding everything.
pub const NORMAL_FRACTION: f64 = 0.1;

/// One-line, human-readable reason a source cannot be stream-copied.
pub fn describe_blocker(b: &CopyBlocker) -> String {
    match b {
        CopyBlocker::CodecMismatch { source, target } =>
            format!("source codec {source} differs from target {target}"),
        CopyBlocker::ProfileIncompatible { source, target } =>
            format!("source profile {source} is above target {target}"),
        CopyBlocker::PixFmtMismatch { source, target } =>
            format!("source pixel format {source} differs from target {target}"),
    }
}

/// Picks an x264 preset that bounds encode time. Work is measured in
/// 1080p30-equivalent seconds; returns the preset and that amount.
pub fn preset_for_work(meta: &VideoMetadata) -> (&'static str, f64) {
    let w = meta.width as f64 * meta.height as f64 * meta.fps * meta.duration_secs
        / (1920.0 * 1080.0 * 30.0);
    let w = if w.is_finite() && w > 0.0 { w } else { 0.0 };
    let preset = if w <= 120.0 {
        "slow"
    } else if w <= 900.0 {
        "medium"
    } else if w <= 3600.0 {
        "fast"
    } else {
        "veryfast"
    };
    (preset, w)
}

/// Decides, per range, whether a segment is stream-copied (`true`) or
/// re-encoded. `ranges` must be in the same timeline as `meta.gop_stats`.
pub fn plan_segments(
    meta: &VideoMetadata,
    ranges: &[(f64, f64)],
    mode: &str,
    encoder: &str,
    crf: u32,
    tolerance: f64,
) -> SegmentPlan {
    let n = ranges.len();
    let target_bitrate = estimate_target_bitrate(meta.width, meta.height, crf, encoder);
    let uniform = |copy: bool| SegmentPlan {
        copy: vec![copy; n],
        bitrate_ratio: vec![None; n],
        blockers: vec![],
        tolerance,
        target_bitrate,
    };
    match mode {
        "copy" => uniform(true),
        "smart" | "smart-auto" => {
            let blockers = check_copy_compatibility(meta, encoder_to_codec(encoder), encoder);
            let tolerance = if mode == "smart-auto" {
                auto_tune_tolerance(&meta.gop_stats, target_bitrate, tolerance)
            } else {
                tolerance
            };
            let bitrate_ratio: Vec<Option<f64>> = ranges
                .iter()
                .map(|&(start, end)| {
                    let rates: Vec<f64> = meta
                        .gop_stats
                        .iter()
                        .filter(|g| g.start_time < end && g.end_time > start)
                        .map(|g| g.bitrate_bps)
                        .collect();
                    if rates.is_empty() {
                        None
                    } else {
                        Some(rates.iter().sum::<f64>() / rates.len() as f64 / target_bitrate)
                    }
                })
                .collect();
            let copy = if blockers.is_empty() {
                bitrate_ratio
                    .iter()
                    .map(|r| matches!(r, Some(r) if *r >= 1.0 - tolerance && *r <= 1.0 + tolerance))
                    .collect()
            } else {
                vec![false; n]
            };
            SegmentPlan { copy, bitrate_ratio, blockers, tolerance, target_bitrate }
        }
        _ => uniform(false),
    }
}

/// Recommends a mode and an x264 preset for transcoding `meta` with `encoder`
/// at `crf`; `max_tolerance` bounds the auto-tuned bitrate tolerance.
pub fn recommend(meta: &VideoMetadata, encoder: &str, crf: u32, max_tolerance: f64) -> Recommendation {
    let (preset, w) = preset_for_work(meta);
    let preset_reason = format!("preset {preset}: about {:.0} min of 1080p30-equivalent work", w / 60.0);
    let blockers = check_copy_compatibility(meta, encoder_to_codec(encoder), encoder);
    let (mode, reason, fraction) = if !blockers.is_empty() {
        let list: Vec<String> = blockers.iter().map(describe_blocker).collect();
        ("normal", format!("re-encode everything: {}", list.join("; ")), None)
    } else if meta.gop_stats.is_empty() {
        ("normal", "no GOP bitrate data; re-encode everything".to_string(), None)
    } else {
        let target = estimate_target_bitrate(meta.width, meta.height, crf, encoder);
        let tol = auto_tune_tolerance(&meta.gop_stats, target, max_tolerance);
        let near = meta
            .gop_stats
            .iter()
            .filter(|g| (g.bitrate_bps / target - 1.0).abs() <= tol)
            .count();
        let f = near as f64 / meta.gop_stats.len() as f64;
        let pct = (f * 100.0).round();
        let tolpct = (tol * 100.0).round();
        if f >= COPY_FRACTION {
            ("copy", format!("{pct}% of GOPs within ±{tolpct}% of the target bitrate; stream-copy"), Some(f))
        } else if f <= NORMAL_FRACTION {
            ("normal", format!("only {pct}% of GOPs near the target bitrate; re-encode everything"), Some(f))
        } else {
            ("smart-auto", format!("{pct}% of GOPs near the target bitrate; copy those, re-encode the rest"), Some(f))
        }
    };
    Recommendation {
        mode: mode.to_string(),
        preset: preset.to_string(),
        reasons: vec![reason, preset_reason],
        copyable_gop_fraction: fraction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(width: u32, height: u32, fps: f64, duration_secs: f64, gop_rates: &[f64]) -> VideoMetadata {
        let gop_stats = gop_rates
            .iter()
            .enumerate()
            .map(|(i, &bitrate_bps)| GopStats {
                start_frame: i as u64 * 25,
                end_frame: i as u64 * 25 + 24,
                start_time: i as f64,
                end_time: i as f64 + 1.0,
                size_bytes: (bitrate_bps / 8.0) as u64,
                bitrate_bps,
            })
            .collect();
        VideoMetadata {
            duration_secs,
            width,
            height,
            fps,
            total_frames: (fps * duration_secs) as u64,
            codec_name: "h264".to_string(),
            keyframe_positions: vec![0],
            keyframe_timestamps: vec![0.0],
            scene_changes: vec![],
            complexity_map: vec![],
            gop_stats,
            profile: Some("High".to_string()),
            pix_fmt: Some("yuv420p".to_string()),
            audio_tracks: vec![],
            subtitle_tracks: vec![],
            chapters: vec![],
        }
    }

    fn target() -> f64 {
        estimate_target_bitrate(1920, 1080, 23, "libx264")
    }

    #[test]
    fn preset_for_work_steps_down_as_work_grows() {
        let p = |w, h, fps, d| preset_for_work(&meta(w, h, fps, d, &[])).0;
        assert_eq!(p(1920, 1080, 30.0, 120.0), "slow");
        assert_eq!(p(1920, 1080, 30.0, 121.0), "medium");
        assert_eq!(p(1920, 1080, 30.0, 900.0), "medium");
        assert_eq!(p(1920, 1080, 30.0, 901.0), "fast");
        assert_eq!(p(1920, 1080, 30.0, 3600.0), "fast");
        assert_eq!(p(1920, 1080, 30.0, 3601.0), "veryfast");
        let (preset, w) = preset_for_work(&meta(3840, 2160, 60.0, 60.0, &[]));
        assert_eq!((preset, w), ("medium", 480.0));
        assert_eq!(preset_for_work(&meta(0, 0, f64::NAN, 10.0, &[])), ("slow", 0.0));
    }

    #[test]
    fn recommend_re_encodes_when_codecs_differ() {
        let r = recommend(&meta(1920, 1080, 30.0, 20.0, &[target(); 20]), "libx265", 23, 0.3);
        assert_eq!(r.mode, "normal");
        assert!(
            r.reasons[0].starts_with("re-encode everything: source codec h264 differs from target hevc"),
            "{:?}",
            r.reasons
        );
        assert_eq!(r.copyable_gop_fraction, None);
    }

    #[test]
    fn recommend_copies_when_gops_sit_on_the_target() {
        let r = recommend(&meta(1920, 1080, 30.0, 20.0, &[target(); 20]), "libx264", 23, 0.3);
        assert_eq!(r.mode, "copy");
        assert_eq!(r.copyable_gop_fraction, Some(1.0));
        assert_eq!(r.preset, "slow");
        assert_eq!(r.reasons.last().unwrap(), "preset slow: about 0 min of 1080p30-equivalent work");
    }

    #[test]
    fn recommend_smart_auto_for_a_mix() {
        let mut rates = vec![target(); 10];
        rates.extend([target() * 5.0; 10]);
        let r = recommend(&meta(1920, 1080, 30.0, 20.0, &rates), "libx264", 23, 0.3);
        assert_eq!(r.mode, "smart-auto");
        assert_eq!(r.copyable_gop_fraction, Some(0.5));
        assert_eq!(r.reasons[0], "50% of GOPs near the target bitrate; copy those, re-encode the rest");
    }

    #[test]
    fn recommend_normal_without_gops() {
        let r = recommend(&meta(1920, 1080, 30.0, 20.0, &[]), "libx264", 23, 0.3);
        assert_eq!(r.mode, "normal");
        assert_eq!(r.reasons[0], "no GOP bitrate data; re-encode everything");
        assert_eq!(r.copyable_gop_fraction, None);
    }

    #[test]
    fn plan_segments_honours_each_mode() {
        let mut rates = vec![target(); 10];
        rates.extend([target() * 5.0; 10]);
        let m = meta(1920, 1080, 30.0, 20.0, &rates);
        let ranges = [(0.0, 10.0), (10.0, 20.0)];
        let plan = |m: &VideoMetadata, mode, enc| plan_segments(m, &ranges, mode, enc, 23, 0.3).copy;
        assert_eq!(plan(&m, "normal", "libx264"), [false, false]);
        assert_eq!(plan(&m, "copy", "libx264"), [true, true]);
        assert_eq!(plan(&m, "smart", "libx264"), [true, false]);
        assert_eq!(plan(&m, "smart-auto", "libx264"), [true, false]);
        assert_eq!(plan(&m, "bogus", "libx264"), [false, false]);
        let p = plan_segments(&m, &ranges, "smart", "libx265", 23, 0.3);
        assert_eq!(p.copy, [false, false]);
        assert_eq!(p.blockers.len(), 1);
    }

    #[test]
    #[ignore = "needs ffmpeg; run in the builder image with --include-ignored"]
    fn fast_analyze_video_reads_a_generated_clip() {
        let dir = std::env::temp_dir().join(format!("media-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.join("source.mp4");
        let status = StdCommand::new("ffmpeg")
            .args(["-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=duration=6:size=320x240:rate=25",
                "-f", "lavfi", "-i", "sine=frequency=440:duration=6",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "25", "-c:a", "aac", "-shortest"])
            .arg(&source)
            .status()
            .unwrap();
        assert!(status.success());
        let m = fast_analyze_video(source.to_str().unwrap()).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!((m.width, m.height), (320, 240));
        assert!((m.fps - 25.0).abs() < 0.01, "fps {}", m.fps);
        assert!((m.duration_secs - 6.0).abs() < 0.1, "duration {}", m.duration_secs);
        assert!(m.gop_stats.len() >= 5, "{} GOPs", m.gop_stats.len());
        assert_eq!(m.audio_tracks.len(), 1);
    }
}
