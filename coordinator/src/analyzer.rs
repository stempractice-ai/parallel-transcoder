extern crate ffmpeg_next as ffmpeg;

use anyhow::{Context, Result};
use ffmpeg::format::input;
use ffmpeg::media::Type;
use ffmpeg::software::scaling::{context::Context as ScalerContext, flag::Flags};
use ffmpeg::util::format::pixel::Pixel;
use ffmpeg::util::frame::video::Video;
use std::path::Path;
use tracing::{debug, info};
use transcoder_cluster::media::VideoMetadata;

const SCENE_CHANGE_THRESHOLD: f64 = 0.30;
const HISTOGRAM_BINS: usize = 64;

/// Analyze a video file and extract metadata for segmentation.
///
/// Performs a single pass through the video to collect:
/// - Basic metadata (resolution, fps, duration)
/// - All keyframe positions
/// - Scene change locations (via histogram comparison)
/// - Per-frame complexity estimates (spatial variance)
pub fn analyze_video(input_path: &str) -> Result<VideoMetadata> {
    let path = Path::new(input_path);
    if !path.exists() {
        anyhow::bail!("Input file does not exist: {}", input_path);
    }

    ffmpeg::init().context("Failed to initialize FFmpeg")?;

    let mut ictx = input(&input_path).context("Failed to open input video")?;

    // Find video stream
    let stream = ictx
        .streams()
        .best(Type::Video)
        .ok_or_else(|| anyhow::anyhow!("No video stream found in {}", input_path))?;

    let video_stream_index = stream.index();
    let time_base = stream.time_base();
    let tb_num = time_base.numerator();
    let tb_den = time_base.denominator();
    let avg_frame_rate = stream.avg_frame_rate();

    let fps = if avg_frame_rate.denominator() != 0 {
        avg_frame_rate.numerator() as f64 / avg_frame_rate.denominator() as f64
    } else {
        30.0
    };

    let stream_duration = stream.duration();
    let duration_secs = if stream_duration > 0 {
        stream_duration as f64 * tb_num as f64 / tb_den as f64
    } else {
        // Fall back to format-level duration
        let fmt_dur = ictx.duration();
        if fmt_dur > 0 {
            fmt_dur as f64 / f64::from(ffmpeg::ffi::AV_TIME_BASE)
        } else {
            0.0
        }
    };

    // Create decoder
    let codec_params = stream.parameters();
    let context_decoder =
        ffmpeg::codec::context::Context::from_parameters(codec_params)
            .context("Failed to create codec context")?;
    let mut decoder = context_decoder
        .decoder()
        .video()
        .context("Failed to open video decoder")?;

    let width = decoder.width();
    let height = decoder.height();
    let codec_name = decoder
        .codec()
        .map(|c| c.name().to_string())
        .unwrap_or_else(|| "unknown".into());

    info!(
        "Analyzing: {}x{} @ {:.2} fps, {:.2}s, codec={}",
        width, height, fps, duration_secs, codec_name
    );

    // Scaler for converting frames to grayscale (Y plane) for analysis
    let mut scaler = ScalerContext::get(
        decoder.format(),
        width,
        height,
        Pixel::GRAY8,
        width,
        height,
        Flags::BILINEAR,
    )
    .context("Failed to create pixel format scaler")?;

    let mut keyframe_positions: Vec<u64> = Vec::new();
    let mut keyframe_timestamps: Vec<f64> = Vec::new();
    let mut scene_changes: Vec<u64> = Vec::new();
    let mut complexity_map: Vec<f32> = Vec::new();

    let mut prev_histogram: Option<[f64; HISTOGRAM_BINS]> = None;
    let mut frame_count: u64 = 0;
    let mut decoded_frame = Video::empty();
    let mut gray_frame = Video::empty();

    // Single pass: iterate all packets
    for (stream_ref, packet) in ictx.packets() {
        if stream_ref.index() != video_stream_index {
            continue;
        }

        decoder.send_packet(&packet)?;

        while decoder.receive_frame(&mut decoded_frame).is_ok() {
            // Keyframe detection
            if decoded_frame.is_key() {
                let pts_secs = pts_to_secs(decoded_frame.pts(), tb_num, tb_den);
                keyframe_positions.push(frame_count);
                keyframe_timestamps.push(pts_secs);
                debug!("Keyframe at frame {} ({:.3}s)", frame_count, pts_secs);
            }

            // Convert to grayscale for analysis
            scaler.run(&decoded_frame, &mut gray_frame)?;
            let gray_data = gray_frame.data(0);
            let stride = gray_frame.stride(0) as usize;

            // Spatial complexity (variance of pixel intensities)
            let complexity =
                spatial_complexity(gray_data, width as usize, height as usize, stride);
            complexity_map.push(complexity);

            // Scene change detection via histogram comparison
            let histogram = compute_histogram(gray_data, width as usize, height as usize, stride);
            if let Some(ref prev) = prev_histogram {
                let diff = histogram_chi_square(prev, &histogram);
                if diff > SCENE_CHANGE_THRESHOLD {
                    scene_changes.push(frame_count);
                    debug!(
                        "Scene change at frame {} (diff={:.4})",
                        frame_count, diff
                    );
                }
            }
            prev_histogram = Some(histogram);

            frame_count += 1;

            if frame_count % 1000 == 0 {
                info!("Analyzed {} frames...", frame_count);
            }
        }
    }

    // Flush decoder
    decoder.send_eof()?;
    while decoder.receive_frame(&mut decoded_frame).is_ok() {
        if decoded_frame.is_key() {
            let pts_secs = pts_to_secs(decoded_frame.pts(), tb_num, tb_den);
            keyframe_positions.push(frame_count);
            keyframe_timestamps.push(pts_secs);
        }

        scaler.run(&decoded_frame, &mut gray_frame)?;
        let gray_data = gray_frame.data(0);
        let stride = gray_frame.stride(0) as usize;

        let complexity = spatial_complexity(gray_data, width as usize, height as usize, stride);
        complexity_map.push(complexity);

        let histogram = compute_histogram(gray_data, width as usize, height as usize, stride);
        if let Some(ref prev) = prev_histogram {
            let diff = histogram_chi_square(prev, &histogram);
            if diff > SCENE_CHANGE_THRESHOLD {
                scene_changes.push(frame_count);
            }
        }
        prev_histogram = Some(histogram);

        frame_count += 1;
    }

    info!(
        "Analysis complete: {} frames, {} keyframes, {} scene changes",
        frame_count,
        keyframe_positions.len(),
        scene_changes.len()
    );

    Ok(VideoMetadata {
        duration_secs,
        width,
        height,
        fps,
        total_frames: frame_count,
        codec_name,
        keyframe_positions,
        keyframe_timestamps,
        scene_changes,
        complexity_map,
        gop_stats: vec![], // only populated by fast_analyze_video
        profile: None,
        pix_fmt: None,
        audio_tracks: vec![],
        subtitle_tracks: vec![],
        chapters: vec![],
    })
}

/// Convert PTS value to seconds using the stream time base.
fn pts_to_secs(pts: Option<i64>, tb_num: i32, tb_den: i32) -> f64 {
    match pts {
        Some(p) => p as f64 * tb_num as f64 / tb_den as f64,
        None => 0.0,
    }
}

/// Compute a normalized histogram of grayscale pixel intensities.
///
/// Uses `HISTOGRAM_BINS` bins, each covering `256 / HISTOGRAM_BINS` intensity levels.
/// The result is normalized so bins sum to 1.0.
fn compute_histogram(
    data: &[u8],
    width: usize,
    height: usize,
    stride: usize,
) -> [f64; HISTOGRAM_BINS] {
    let mut bins = [0u64; HISTOGRAM_BINS];
    let bin_width = 256 / HISTOGRAM_BINS;

    for y in 0..height {
        let row = &data[y * stride..y * stride + width];
        for &pixel in row {
            let bin = (pixel as usize) / bin_width;
            let bin = bin.min(HISTOGRAM_BINS - 1);
            bins[bin] += 1;
        }
    }

    let total = (width * height) as f64;
    let mut normalized = [0.0f64; HISTOGRAM_BINS];
    for i in 0..HISTOGRAM_BINS {
        normalized[i] = bins[i] as f64 / total;
    }
    normalized
}

/// Chi-square distance between two histograms.
///
/// Returns a value in [0, 1+] where 0 means identical and higher means more different.
/// Good for scene change detection — values above ~0.3 typically indicate a scene cut.
fn histogram_chi_square(h1: &[f64; HISTOGRAM_BINS], h2: &[f64; HISTOGRAM_BINS]) -> f64 {
    let mut chi2 = 0.0;
    for i in 0..HISTOGRAM_BINS {
        let sum = h1[i] + h2[i];
        if sum > 1e-10 {
            let diff = h1[i] - h2[i];
            chi2 += (diff * diff) / sum;
        }
    }
    chi2 / 2.0 // Normalize to [0, 1] range for identical-length histograms
}

/// Calculate spatial complexity of a grayscale frame.
///
/// Uses the variance of pixel intensities as a proxy — higher variance means more
/// visual detail/texture which requires more bits to encode.
/// Returns a value normalized to approximately [0.0, 1.0].
fn spatial_complexity(data: &[u8], width: usize, height: usize, stride: usize) -> f32 {
    let pixel_count = (width * height) as f64;
    if pixel_count == 0.0 {
        return 0.0;
    }

    // Compute mean
    let mut sum = 0u64;
    for y in 0..height {
        let row = &data[y * stride..y * stride + width];
        for &pixel in row {
            sum += pixel as u64;
        }
    }
    let mean = sum as f64 / pixel_count;

    // Compute variance
    let mut var_sum = 0.0f64;
    for y in 0..height {
        let row = &data[y * stride..y * stride + width];
        for &pixel in row {
            let diff = pixel as f64 - mean;
            var_sum += diff * diff;
        }
    }
    let variance = var_sum / pixel_count;

    // Normalize: max possible variance for 8-bit is ~(127.5)^2 ≈ 16256
    // Typical video frames have variance in 200-4000 range
    let normalized = (variance / 4000.0).min(1.0) as f32;
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_identical() {
        let h = [1.0 / HISTOGRAM_BINS as f64; HISTOGRAM_BINS];
        let diff = histogram_chi_square(&h, &h);
        assert!(diff.abs() < 1e-10, "Identical histograms should have zero distance");
    }

    #[test]
    fn test_histogram_different() {
        let mut h1 = [0.0f64; HISTOGRAM_BINS];
        let mut h2 = [0.0f64; HISTOGRAM_BINS];
        // Concentrate all weight at opposite ends
        h1[0] = 1.0;
        h2[HISTOGRAM_BINS - 1] = 1.0;
        let diff = histogram_chi_square(&h1, &h2);
        assert!(
            diff > SCENE_CHANGE_THRESHOLD,
            "Maximally different histograms should exceed scene change threshold"
        );
    }

    #[test]
    fn test_spatial_complexity_flat() {
        // Uniform gray image — low complexity
        let width = 64;
        let height = 64;
        let data = vec![128u8; width * height];
        let complexity = spatial_complexity(&data, width, height, width);
        assert!(
            complexity < 0.01,
            "Flat image should have near-zero complexity, got {}",
            complexity
        );
    }

    #[test]
    fn test_spatial_complexity_noisy() {
        // Alternating black/white pixels — high complexity
        let width = 64;
        let height = 64;
        let data: Vec<u8> = (0..width * height)
            .map(|i| if i % 2 == 0 { 0 } else { 255 })
            .collect();
        let complexity = spatial_complexity(&data, width, height, width);
        assert!(
            complexity > 0.5,
            "Noisy image should have high complexity, got {}",
            complexity
        );
    }

    #[test]
    fn test_compute_histogram_uniform() {
        // All pixels same value — should concentrate in one bin
        let width = 64;
        let height = 64;
        let data = vec![100u8; width * height];
        let hist = compute_histogram(&data, width, height, width);
        let expected_bin = 100 / (256 / HISTOGRAM_BINS);
        assert!(
            (hist[expected_bin] - 1.0).abs() < 1e-10,
            "All pixels should be in bin {}",
            expected_bin
        );
    }

    #[test]
    fn test_pts_to_secs() {
        assert!((pts_to_secs(Some(90000), 1, 90000) - 1.0).abs() < 1e-10);
        assert!((pts_to_secs(Some(48000), 1, 48000) - 1.0).abs() < 1e-10);
        assert!((pts_to_secs(None, 1, 90000)).abs() < 1e-10);
    }
}
