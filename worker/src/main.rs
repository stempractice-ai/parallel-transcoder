extern crate ffmpeg_next as ffmpeg;
extern crate ffmpeg_sys_next as sys;

use anyhow::{Context, Result};
use clap::Parser;
use ffmpeg::codec;
use ffmpeg::encoder;
use ffmpeg::format::{self, input, Pixel};
use ffmpeg::media::Type;
use ffmpeg::software::scaling::{context::Context as ScalerContext, flag::Flags};
use ffmpeg::util::frame::video::Video;
use ffmpeg::{Dictionary, Rational};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{debug, info, warn};

#[derive(Parser, Debug)]
#[command(name = "transcoder-worker")]
#[command(about = "Worker process for parallel video transcoding")]
struct Args {
    /// Segment descriptor as JSON string
    #[arg(short, long)]
    segment: Option<String>,

    /// Input video file
    #[arg(short, long)]
    input: String,

    /// Output segment file (.ts for MPEG-TS)
    #[arg(short, long)]
    output: String,

    /// Worker ID
    #[arg(short, long)]
    worker_id: usize,

    /// Target video bitrate in kbps (0 = CRF mode)
    #[arg(short, long, default_value = "0")]
    bitrate: u32,

    /// CRF value for quality-based encoding (lower = better quality)
    #[arg(short, long, default_value = "23")]
    crf: u32,

    /// Encoding preset (ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow)
    #[arg(short, long, default_value = "medium")]
    preset: String,

    /// Encoder to use (libx264, libx265, libsvtav1, libaom-av1, h264_videotoolbox, hevc_videotoolbox)
    #[arg(short, long, default_value = "libx264")]
    encoder: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Pre-split mode: input file contains only this segment's data (skip seeking/filtering)
    #[arg(long)]
    presplit: bool,

    /// Enable VideoToolbox hardware decoding (macOS only)
    #[arg(long)]
    hw_decode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub id: usize,
    pub start_frame: u64,
    pub end_frame: u64,
    pub start_timestamp: f64,
    pub end_timestamp: f64,
    #[serde(default)]
    pub lookahead_frames: Option<usize>,
    pub complexity_estimate: f32,
    pub scene_changes: Vec<u64>,
}

#[derive(Debug, Serialize)]
struct WorkerResult {
    segment_id: usize,
    worker_id: usize,
    frames_encoded: u64,
    output_size_bytes: u64,
    encoding_time_secs: f64,
    average_complexity: f32,
    scene_changes_detected: u64,
    output_path: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let log_level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_writer(std::io::stderr)
        .init();

    info!("Worker {} starting", args.worker_id);

    let segment: SegmentDescriptor = if let Some(ref json) = args.segment {
        serde_json::from_str(json).context("Failed to parse segment descriptor JSON")?
    } else {
        info!("Reading segment descriptor from stdin...");
        serde_json::from_reader(std::io::stdin()).context("Failed to read segment from stdin")?
    };

    info!(
        "Segment {}: frames {}-{} ({:.2}s-{:.2}s)",
        segment.id, segment.start_frame, segment.end_frame,
        segment.start_timestamp, segment.end_timestamp
    );

    let start_time = Instant::now();

    let frames_encoded = transcode_segment(&args, &segment)?;

    let encoding_time = start_time.elapsed().as_secs_f64();
    let output_size = std::fs::metadata(&args.output)
        .map(|m| m.len())
        .unwrap_or(0);

    let worker_result = WorkerResult {
        segment_id: segment.id,
        worker_id: args.worker_id,
        frames_encoded,
        output_size_bytes: output_size,
        encoding_time_secs: encoding_time,
        average_complexity: segment.complexity_estimate,
        scene_changes_detected: 0,
        output_path: args.output.clone(),
    };

    // Write result JSON to stdout for coordinator to collect
    println!("{}", serde_json::to_string(&worker_result)?);

    info!(
        "Worker {} complete: {} frames in {:.2}s ({:.1} fps)",
        args.worker_id,
        frames_encoded,
        encoding_time,
        frames_encoded as f64 / encoding_time
    );

    Ok(())
}

/// Encode a frame and write packets to output.
fn encode_and_write(
    encoder: &mut ffmpeg::encoder::video::Video,
    packet: &mut ffmpeg::Packet,
    frame: &Video,
    encoder_tb: Rational,
    output_tb: Rational,
    octx: &mut format::context::Output,
) -> Result<()> {
    encoder.send_frame(frame)?;
    receive_and_write(encoder, packet, encoder_tb, output_tb, octx)
}

/// Receive all pending packets from encoder and write to output.
fn receive_and_write(
    encoder: &mut ffmpeg::encoder::video::Video,
    packet: &mut ffmpeg::Packet,
    encoder_tb: Rational,
    output_tb: Rational,
    octx: &mut format::context::Output,
) -> Result<()> {
    while encoder.receive_packet(packet).is_ok() {
        packet.rescale_ts(encoder_tb, output_tb);
        packet.set_stream(0);
        packet.write_interleaved(octx)?;
    }
    Ok(())
}

/// Stream-copy one audio packet from input to output — no decode/re-encode,
/// just timestamp rescaling and remuxing onto the output audio stream.
/// Outside presplit mode, packets outside the segment's time window are
/// dropped to match the video path's frame filtering.
fn copy_audio_packet(
    mut packet: ffmpeg::Packet,
    input_tb: Rational,
    output_stream_index: usize,
    presplit: bool,
    segment: &SegmentDescriptor,
    octx: &mut format::context::Output,
) -> Result<()> {
    if !presplit {
        if let Some(pts) = packet.pts() {
            let pts_secs =
                pts as f64 * input_tb.numerator() as f64 / input_tb.denominator() as f64;
            if pts_secs < segment.start_timestamp - 0.001 || pts_secs > segment.end_timestamp + 0.001 {
                return Ok(());
            }
        }
    }
    let output_tb = octx.stream(output_stream_index).unwrap().time_base();
    packet.rescale_ts(input_tb, output_tb);
    packet.set_stream(output_stream_index);
    packet.write_interleaved(octx)?;
    Ok(())
}

fn transcode_segment(
    args: &Args,
    segment: &SegmentDescriptor,
) -> Result<u64> {
    let presplit = args.presplit;
    let hw_decode = args.hw_decode;

    ffmpeg::init().context("Failed to initialize FFmpeg")?;

    // --- Open input ---
    let mut ictx = input(&args.input).context("Failed to open input video")?;

    let input_stream = ictx
        .streams()
        .best(Type::Video)
        .ok_or_else(|| anyhow::anyhow!("No video stream found"))?;

    let video_stream_index = input_stream.index();
    let input_time_base = input_stream.time_base();
    let avg_frame_rate = input_stream.avg_frame_rate();

    let codec_params = input_stream.parameters();
    let decoder_ctx = codec::context::Context::from_parameters(codec_params)
        .context("Failed to create decoder context")?;
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .context("Failed to open video decoder")?;

    let width = decoder.width();
    let height = decoder.height();
    let src_format = decoder.format();

    // --- Hardware decode setup (VideoToolbox) ---
    let mut _hw_ctx: *mut sys::AVBufferRef = std::ptr::null_mut();
    if hw_decode {
        info!("Enabling VideoToolbox hardware decoding");
        unsafe {
            let ret = sys::av_hwdevice_ctx_create(
                &mut _hw_ctx,
                sys::AVHWDeviceType::AV_HWDEVICE_TYPE_VIDEOTOOLBOX,
                std::ptr::null(),
                std::ptr::null_mut(),
                0,
            );
            if ret < 0 {
                warn!("Failed to create VideoToolbox device context (err={}), falling back to software decode", ret);
            } else {
                (*decoder.as_mut_ptr()).hw_device_ctx = sys::av_buffer_ref(_hw_ctx);
                info!("VideoToolbox hardware decode enabled");
            }
        }
    }

    info!("Input: {}x{} {:?}", width, height, src_format);

    // --- Scaler ---
    // Convert decoded frames to target pixel format for encoding.
    // For hw decode, defer scaler init until we see the actual frame format.
    // enc_format may be overridden below for 10-bit HEVC/AV1.
    let enc_format = Pixel::YUV420P;

    let mut yuv_scaler: Option<ScalerContext> = if !hw_decode {
        Some(ScalerContext::get(
            src_format, width, height,
            enc_format, width, height,
            Flags::BILINEAR,
        ).context("Failed to create scaler")?)
    } else {
        None
    };

    // --- Set up encoder ---
    let is_videotoolbox = args.encoder.contains("videotoolbox");
    let is_nvenc = args.encoder.contains("nvenc");
    let is_vaapi = args.encoder.contains("vaapi");
    let is_hevc = args.encoder.contains("x265") || args.encoder.contains("hevc");
    let is_av1 = args.encoder.contains("svtav1") || args.encoder.contains("aom-av1") || args.encoder == "av1_nvenc";

    let video_codec = find_encoder(&args.encoder)?;

    // For AV1 and HEVC, prefer YUV420P10LE when the source is 10-bit
    let enc_format = if (is_hevc || is_av1) && matches!(src_format, Pixel::YUV420P10LE | Pixel::YUV420P10BE | Pixel::P010LE | Pixel::P010BE) {
        Pixel::YUV420P10LE
    } else {
        Pixel::YUV420P
    };

    // Re-create scaler if we changed enc_format to 10-bit
    if !hw_decode {
        yuv_scaler = Some(ScalerContext::get(
            src_format, width, height,
            enc_format, width, height,
            Flags::BILINEAR,
        ).context("Failed to create scaler for target pixel format")?);
    }

    let mut octx = format::output(&args.output).context("Failed to create output file")?;
    let global_header = octx.format().flags().contains(format::Flags::GLOBAL_HEADER);
    let mut ost = octx.add_stream(video_codec).context("Failed to add output stream")?;

    // Compute encoder time base from frame rate, with fallback for invalid rates
    let enc_time_base = if avg_frame_rate.numerator() > 0 && avg_frame_rate.denominator() > 0 {
        Rational::new(avg_frame_rate.denominator(), avg_frame_rate.numerator())
    } else {
        // Fallback: use 1/30 if frame rate is unknown, or derive from input time base
        let r_frame_rate = unsafe { (*input_stream.as_ptr()).r_frame_rate };
        if r_frame_rate.num > 0 && r_frame_rate.den > 0 {
            Rational::new(r_frame_rate.den, r_frame_rate.num)
        } else {
            info!("Warning: could not determine frame rate, defaulting to 1/30");
            Rational::new(1, 30)
        }
    };

    let enc_ctx = codec::context::Context::new_with_codec(video_codec);
    let mut video_enc = enc_ctx.encoder().video()?;
    video_enc.set_width(width);
    video_enc.set_height(height);
    video_enc.set_format(enc_format);
    video_enc.set_time_base(enc_time_base);
    video_enc.set_frame_rate(Some(avg_frame_rate));

    let mut opts = Dictionary::new();
    if is_videotoolbox {
        // VideoToolbox (H.264 or HEVC hardware encoder — macOS)
        let target_bitrate = if args.bitrate > 0 {
            args.bitrate as usize * 1000
        } else {
            let pixels = width as usize * height as usize;
            if pixels >= 3840 * 2160 {
                if is_hevc { 30_000_000 } else { 50_000_000 }
            } else if pixels >= 1920 * 1080 {
                if is_hevc { 10_000_000 } else { 20_000_000 }
            } else if pixels >= 1280 * 720 {
                if is_hevc { 5_000_000 } else { 10_000_000 }
            } else {
                if is_hevc { 2_500_000 } else { 5_000_000 }
            }
        };
        video_enc.set_bit_rate(target_bitrate);
        opts.set("profile", if is_hevc { "main" } else { "high" });
        opts.set("realtime", "false");
        opts.set("allow_sw", "false");
        info!("{} encoder: bitrate={} bps", args.encoder, target_bitrate);
    } else if is_nvenc {
        // NVENC (NVIDIA GPU — Linux)
        let target_bitrate = if args.bitrate > 0 {
            args.bitrate as usize * 1000
        } else {
            let pixels = width as usize * height as usize;
            let base = if pixels >= 3840 * 2160 { 40_000_000 }
                else if pixels >= 1920 * 1080 { 15_000_000 }
                else if pixels >= 1280 * 720 { 8_000_000 }
                else { 4_000_000 };
            if is_hevc || is_av1 { base * 2 / 3 } else { base }
        };
        video_enc.set_bit_rate(target_bitrate);
        opts.set("preset", "p5");  // NVENC preset (p1=fastest .. p7=slowest)
        opts.set("rc", "vbr");
        if is_hevc {
            opts.set("profile", "main");
        } else if !is_av1 {
            opts.set("profile", "high");
        }
        info!("{} encoder: bitrate={} bps, preset=p5", args.encoder, target_bitrate);
    } else if is_vaapi {
        // VAAPI (Intel/AMD GPU — Linux)
        let target_bitrate = if args.bitrate > 0 {
            args.bitrate as usize * 1000
        } else {
            let pixels = width as usize * height as usize;
            let base = if pixels >= 3840 * 2160 { 40_000_000 }
                else if pixels >= 1920 * 1080 { 15_000_000 }
                else if pixels >= 1280 * 720 { 8_000_000 }
                else { 4_000_000 };
            if is_hevc { base * 2 / 3 } else { base }
        };
        video_enc.set_bit_rate(target_bitrate);
        opts.set("rc_mode", "VBR");
        if is_hevc {
            opts.set("profile", "main");
        } else {
            opts.set("profile", "high");
        }
        info!("{} encoder: bitrate={} bps", args.encoder, target_bitrate);
    } else if is_av1 {
        // AV1 encoders (libsvtav1 or libaom-av1)
        if args.encoder == "libsvtav1" {
            // SVT-AV1: CRF mode, preset 0-13 (map x264 presets to SVT-AV1 presets)
            let svt_preset = match args.preset.as_str() {
                "ultrafast" => "12",
                "superfast" => "10",
                "veryfast" => "8",
                "faster" => "7",
                "fast" => "6",
                "medium" => "5",
                "slow" => "4",
                "slower" => "2",
                "veryslow" => "0",
                other => other,  // allow direct numeric presets
            };
            opts.set("preset", svt_preset);
            if args.bitrate > 0 {
                video_enc.set_bit_rate(args.bitrate as usize * 1000);
            } else {
                opts.set("crf", &args.crf.to_string());
            }
            info!("SVT-AV1 encoder: preset={}, crf={}", svt_preset, args.crf);
        } else {
            // libaom-av1: CRF mode, cpu-used 0-8
            let cpu_used = match args.preset.as_str() {
                "ultrafast" => "8",
                "superfast" => "7",
                "veryfast" => "6",
                "faster" => "5",
                "fast" => "4",
                "medium" => "3",
                "slow" => "2",
                "slower" => "1",
                "veryslow" => "0",
                other => other,
            };
            opts.set("cpu-used", cpu_used);
            opts.set("row-mt", "1");
            opts.set("tiles", "2x2");
            if args.bitrate > 0 {
                video_enc.set_bit_rate(args.bitrate as usize * 1000);
            } else {
                opts.set("crf", &args.crf.to_string());
            }
            info!("libaom-av1 encoder: cpu-used={}, crf={}", cpu_used, args.crf);
        }
    } else if is_hevc {
        // libx265: CRF mode with preset
        opts.set("preset", &args.preset);
        if args.bitrate > 0 {
            video_enc.set_bit_rate(args.bitrate as usize * 1000);
        } else {
            opts.set("crf", &args.crf.to_string());
        }
        info!("libx265 encoder: preset={}, crf={}", args.preset, args.crf);
    } else {
        // libx264: CRF mode with preset
        opts.set("preset", &args.preset);
        if args.bitrate > 0 {
            video_enc.set_bit_rate(args.bitrate as usize * 1000);
        } else {
            opts.set("crf", &args.crf.to_string());
        }
        info!("libx264 encoder: preset={}, crf={}", args.preset, args.crf);
    }

    if global_header {
        video_enc.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut opened_encoder = video_enc
        .open_as_with(video_codec, opts)
        .context("Failed to open encoder")?;
    ost.set_parameters(&opened_encoder);

    let encoder_tb = opened_encoder.time_base();

    // --- Optional audio passthrough (stream copy, no re-encode) ---
    // (index_in, time_base_in, index_out), or None if the source has no
    // audio track or the muxer has no matching codec support for it.
    let audio_info: Option<(usize, Rational, usize)> = {
        match ictx.streams().best(Type::Audio) {
            Some(a_stream) => {
                let a_index = a_stream.index();
                let a_tb = a_stream.time_base();
                let params = a_stream.parameters();
                match ffmpeg::encoder::find(params.id()) {
                    Some(audio_codec) => {
                        let mut aost = octx
                            .add_stream(audio_codec)
                            .context("Failed to add output audio stream")?;
                        aost.set_parameters(params);
                        // Standard remux gotcha: a stale codec_tag from the
                        // source container can confuse the target muxer.
                        unsafe {
                            (*aost.parameters().as_mut_ptr()).codec_tag = 0;
                        }
                        let out_index = aost.index();
                        info!("Audio passthrough: input stream {} -> output stream {}", a_index, out_index);
                        Some((a_index, a_tb, out_index))
                    }
                    None => {
                        warn!("No muxer support for input audio codec, dropping audio track");
                        None
                    }
                }
            }
            None => None,
        }
    };

    octx.write_header().context("Failed to write output header")?;

    // Re-read output_tb after write_header (muxer may change it)
    let output_tb = octx.stream(0).unwrap().time_base();

    info!(
        "Encoder ready: {} preset={} crf={}, encoder_tb={}/{}, output_tb={}/{}",
        args.encoder, args.preset, args.crf,
        encoder_tb.numerator(), encoder_tb.denominator(),
        output_tb.numerator(), output_tb.denominator()
    );

    // --- Seek to segment start ---
    if !presplit && segment.start_timestamp > 0.1 {
        let seek_target =
            (segment.start_timestamp * f64::from(ffmpeg::ffi::AV_TIME_BASE)) as i64;
        ictx.seek(seek_target, ..seek_target)
            .context("Failed to seek to segment start")?;
        decoder.flush();
        info!("Seeked to {:.2}s", segment.start_timestamp);
    }

    // --- Decode and encode frames ---
    // Stream decode → scale → encode without buffering.
    // x264's built-in scenecut detection and rate control handle everything.
    let mut frames_encoded: u64 = 0;
    let mut reached_segment = presplit || segment.start_timestamp < 0.1;
    let mut decoded_frame = Video::empty();
    let mut hw_transfer_frame = Video::empty();
    let mut scaler_initialized = !hw_decode;
    let mut enc_packet = ffmpeg::Packet::empty();

    // Helper closure-like processing for each decoded frame
    let process_frame = |frame_ref: &Video,
                             yuv_scaler: &mut Option<ScalerContext>,
                             opened_encoder: &mut ffmpeg::encoder::video::Video,
                             enc_packet: &mut ffmpeg::Packet,
                             octx: &mut format::context::Output,
                             frames_encoded: &mut u64|
     -> Result<()> {
        let ys = yuv_scaler.as_mut().unwrap();
        let mut yuv_frame = Video::empty();
        ys.run(frame_ref, &mut yuv_frame)?;
        yuv_frame.set_pts(Some(*frames_encoded as i64));

        encode_and_write(opened_encoder, enc_packet, &yuv_frame, encoder_tb, output_tb, octx)?;
        *frames_encoded += 1;

        if *frames_encoded % 100 == 0 {
            info!("Encoded {} frames...", *frames_encoded);
        }
        Ok(())
    };

    for (stream, packet) in ictx.packets() {
        let stream_index = stream.index();
        if stream_index != video_stream_index {
            if let Some((audio_index, audio_tb, audio_out_index)) = audio_info {
                if stream_index == audio_index {
                    copy_audio_packet(packet, audio_tb, audio_out_index, presplit, segment, &mut octx)?;
                }
            }
            continue;
        }

        decoder.send_packet(&packet)?;

        while decoder.receive_frame(&mut decoded_frame).is_ok() {
            // Handle hardware decode: transfer from GPU to CPU memory
            let frame_ref = if hw_decode {
                let frame_format = unsafe { (*decoded_frame.as_ptr()).format };
                if frame_format == sys::AVPixelFormat::AV_PIX_FMT_VIDEOTOOLBOX as i32 {
                    unsafe {
                        let ret = sys::av_hwframe_transfer_data(
                            hw_transfer_frame.as_mut_ptr(),
                            decoded_frame.as_ptr(),
                            0,
                        );
                        if ret < 0 {
                            warn!("Failed to transfer hw frame (err={}), skipping", ret);
                            continue;
                        }
                        (*hw_transfer_frame.as_mut_ptr()).pts = (*decoded_frame.as_ptr()).pts;
                    }
                    &hw_transfer_frame
                } else {
                    &decoded_frame
                }
            } else {
                &decoded_frame
            };

            // Lazy-init scaler after seeing actual decoded frame format
            if !scaler_initialized {
                let actual_format = frame_ref.format();
                info!("HW decoded frame format: {:?}, initializing scaler", actual_format);
                yuv_scaler = Some(ScalerContext::get(
                    actual_format, width, height,
                    enc_format, width, height,
                    Flags::BILINEAR,
                ).context("Failed to create YUV420P scaler for hw decode format")?);
                scaler_initialized = true;
            }

            // In presplit mode, accept all frames without timestamp filtering
            if !presplit {
                let pts = frame_ref.pts().unwrap_or(0);
                let pts_secs = pts as f64 * input_time_base.numerator() as f64
                    / input_time_base.denominator() as f64;

                if !reached_segment {
                    if pts_secs < segment.start_timestamp - 0.001 {
                        continue;
                    }
                    reached_segment = true;
                    debug!("Reached segment start at PTS {:.3}s", pts_secs);
                }

                if pts_secs > segment.end_timestamp + 0.001 {
                    break;
                }
            }

            process_frame(
                frame_ref, &mut yuv_scaler, &mut opened_encoder,
                &mut enc_packet, &mut octx, &mut frames_encoded,
            )?;
        }
    }

    // Flush decoder
    decoder.send_eof()?;
    while decoder.receive_frame(&mut decoded_frame).is_ok() {
        let frame_ref = if hw_decode {
            let frame_format = unsafe { (*decoded_frame.as_ptr()).format };
            if frame_format == sys::AVPixelFormat::AV_PIX_FMT_VIDEOTOOLBOX as i32 {
                unsafe {
                    let ret = sys::av_hwframe_transfer_data(
                        hw_transfer_frame.as_mut_ptr(),
                        decoded_frame.as_ptr(),
                        0,
                    );
                    if ret < 0 { continue; }
                    (*hw_transfer_frame.as_mut_ptr()).pts = (*decoded_frame.as_ptr()).pts;
                }
                &hw_transfer_frame
            } else {
                &decoded_frame
            }
        } else {
            &decoded_frame
        };

        if !presplit {
            if !reached_segment { continue; }
            let pts = frame_ref.pts().unwrap_or(0);
            let pts_secs = pts as f64 * input_time_base.numerator() as f64
                / input_time_base.denominator() as f64;
            if pts_secs > segment.end_timestamp + 0.001 { break; }
        }

        process_frame(
            frame_ref, &mut yuv_scaler, &mut opened_encoder,
            &mut enc_packet, &mut octx, &mut frames_encoded,
        )?;
    }

    // Flush encoder
    opened_encoder.send_eof()?;
    receive_and_write(
        &mut opened_encoder,
        &mut enc_packet,
        encoder_tb,
        output_tb,
        &mut octx,
    )?;

    octx.write_trailer()
        .context("Failed to write output trailer")?;

    info!("Encoded {} frames", frames_encoded);

    Ok(frames_encoded)
}

/// Find the FFmpeg encoder by name, with fallbacks for each codec family.
fn find_encoder(name: &str) -> anyhow::Result<ffmpeg::Codec> {
    match name {
        // H.264 — CPU
        "libx264" => encoder::find_by_name("libx264")
            .or_else(|| encoder::find(codec::Id::H264))
            .ok_or_else(|| anyhow::anyhow!("H.264 encoder not found — is libx264 available?")),
        // H.264 — macOS GPU
        "h264_videotoolbox" | "videotoolbox" => encoder::find_by_name("h264_videotoolbox")
            .ok_or_else(|| anyhow::anyhow!("h264_videotoolbox not found — macOS only")),
        // H.264 — NVIDIA GPU (Linux)
        "h264_nvenc" => encoder::find_by_name("h264_nvenc")
            .ok_or_else(|| anyhow::anyhow!("h264_nvenc not found — requires NVIDIA GPU + NVENC SDK")),
        // H.264 — VAAPI (Linux Intel/AMD)
        "h264_vaapi" => encoder::find_by_name("h264_vaapi")
            .ok_or_else(|| anyhow::anyhow!("h264_vaapi not found — requires VAAPI-capable GPU + libva")),

        // H.265 / HEVC — CPU
        "libx265" => encoder::find_by_name("libx265")
            .or_else(|| encoder::find(codec::Id::HEVC))
            .ok_or_else(|| anyhow::anyhow!("H.265 encoder not found — is libx265 available?")),
        // HEVC — macOS GPU
        "hevc_videotoolbox" => encoder::find_by_name("hevc_videotoolbox")
            .ok_or_else(|| anyhow::anyhow!("hevc_videotoolbox not found — macOS only")),
        // HEVC — NVIDIA GPU (Linux)
        "hevc_nvenc" => encoder::find_by_name("hevc_nvenc")
            .ok_or_else(|| anyhow::anyhow!("hevc_nvenc not found — requires NVIDIA GPU + NVENC SDK")),
        // HEVC — VAAPI (Linux Intel/AMD)
        "hevc_vaapi" => encoder::find_by_name("hevc_vaapi")
            .ok_or_else(|| anyhow::anyhow!("hevc_vaapi not found — requires VAAPI-capable GPU + libva")),

        // AV1 — CPU
        "libsvtav1" => encoder::find_by_name("libsvtav1")
            .ok_or_else(|| anyhow::anyhow!("SVT-AV1 encoder not found — is libsvtav1 available?")),
        "libaom-av1" => encoder::find_by_name("libaom-av1")
            .ok_or_else(|| anyhow::anyhow!("libaom-av1 encoder not found — is libaom available?")),
        // AV1 — NVIDIA GPU (Linux, RTX 40+ series)
        "av1_nvenc" => encoder::find_by_name("av1_nvenc")
            .ok_or_else(|| anyhow::anyhow!("av1_nvenc not found — requires NVIDIA RTX 40+ GPU")),

        // Generic fallback: try find_by_name
        other => encoder::find_by_name(other)
            .ok_or_else(|| anyhow::anyhow!("Encoder '{}' not found", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_descriptor_deserialize() {
        let json = r#"{
            "id": 0,
            "start_frame": 0,
            "end_frame": 299,
            "start_timestamp": 0.0,
            "end_timestamp": 10.0,
            "complexity_estimate": 0.5,
            "scene_changes": [90, 180]
        }"#;
        let seg: SegmentDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(seg.id, 0);
        assert_eq!(seg.start_frame, 0);
        assert_eq!(seg.end_frame, 299);
        assert_eq!(seg.scene_changes.len(), 2);
        assert!(seg.lookahead_frames.is_none());
    }

    #[test]
    fn test_segment_descriptor_with_lookahead() {
        let json = r#"{
            "id": 1,
            "start_frame": 300,
            "end_frame": 599,
            "start_timestamp": 10.0,
            "end_timestamp": 20.0,
            "lookahead_frames": 60,
            "complexity_estimate": 0.7,
            "scene_changes": []
        }"#;
        let seg: SegmentDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(seg.lookahead_frames, Some(60));
    }

    #[test]
    fn test_worker_result_serialize() {
        let result = WorkerResult {
            segment_id: 0,
            worker_id: 1,
            frames_encoded: 300,
            output_size_bytes: 1024000,
            encoding_time_secs: 5.5,
            average_complexity: 0.42,
            scene_changes_detected: 2,
            output_path: "/tmp/seg_0.ts".into(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"frames_encoded\":300"));
        assert!(json.contains("\"worker_id\":1"));
    }
}
