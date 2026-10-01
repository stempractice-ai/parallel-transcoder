use anyhow::Result;
use serde::{Deserialize, Serialize};

use transcoder_cluster::media::VideoMetadata;

/// Represents a video segment for parallel processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: usize,
    pub start_frame: u64,
    pub end_frame: u64,
    pub start_timestamp: f64,
    pub end_timestamp: f64,
    pub complexity_estimate: f32,
    pub contains_scene_changes: Vec<u64>,
}

/// Create video segments aligned to keyframe boundaries.
///
/// Segments are split at keyframe positions closest to the target duration boundaries.
/// Each segment includes the average complexity estimate for workload balancing.
pub fn create_segments(
    metadata: &VideoMetadata,
    target_duration_secs: f64,
) -> Result<Vec<Segment>> {
    if metadata.total_frames == 0 {
        anyhow::bail!("Video has no frames to segment");
    }
    if metadata.keyframe_positions.is_empty() {
        anyhow::bail!("No keyframes detected — cannot create aligned segments");
    }

    let target_frames = (target_duration_secs * metadata.fps).round() as u64;
    if target_frames == 0 {
        anyhow::bail!("Target segment duration too small for video frame rate");
    }

    // Build segments by finding keyframes nearest to each target boundary
    let mut segments = Vec::new();
    let mut seg_start_idx = 0; // Index into keyframe_positions for current segment start

    loop {
        let start_frame = metadata.keyframe_positions[seg_start_idx];
        let start_timestamp = metadata.keyframe_timestamps[seg_start_idx];

        // Target end frame for this segment
        let ideal_end = start_frame + target_frames;

        if ideal_end >= metadata.total_frames {
            // Last segment — extends to end of video
            let end_frame = metadata.total_frames.saturating_sub(1);
            let end_timestamp = metadata.duration_secs;

            let seg = build_segment(
                segments.len(),
                start_frame,
                end_frame,
                start_timestamp,
                end_timestamp,
                metadata,
            );
            segments.push(seg);
            break;
        }

        // Find the keyframe closest to ideal_end
        let next_kf_idx = find_nearest_keyframe(&metadata.keyframe_positions, ideal_end);

        // The segment ends just before the next keyframe (which starts the next segment)
        if next_kf_idx <= seg_start_idx {
            // Edge case: no keyframe found after current position, take the next one
            let next_idx = (seg_start_idx + 1).min(metadata.keyframe_positions.len() - 1);
            if next_idx == seg_start_idx {
                // Only one keyframe left — final segment
                let end_frame = metadata.total_frames.saturating_sub(1);
                let end_timestamp = metadata.duration_secs;
                let seg = build_segment(
                    segments.len(),
                    start_frame,
                    end_frame,
                    start_timestamp,
                    end_timestamp,
                    metadata,
                );
                segments.push(seg);
                break;
            }
            seg_start_idx = next_idx;
            let end_frame = metadata.keyframe_positions[seg_start_idx] - 1;
            let end_timestamp = metadata.keyframe_timestamps[seg_start_idx];
            let seg = build_segment(
                segments.len(),
                start_frame,
                end_frame,
                start_timestamp,
                end_timestamp,
                metadata,
            );
            segments.push(seg);
        } else {
            let end_frame = metadata.keyframe_positions[next_kf_idx] - 1;
            let end_timestamp = metadata.keyframe_timestamps[next_kf_idx];
            let seg = build_segment(
                segments.len(),
                start_frame,
                end_frame,
                start_timestamp,
                end_timestamp,
                metadata,
            );
            segments.push(seg);
            seg_start_idx = next_kf_idx;
        }

        if seg_start_idx >= metadata.keyframe_positions.len() - 1 {
            // Remaining frames after last keyframe
            let start_frame = metadata.keyframe_positions[seg_start_idx];
            let start_timestamp = metadata.keyframe_timestamps[seg_start_idx];
            let end_frame = metadata.total_frames.saturating_sub(1);
            if end_frame > start_frame {
                let seg = build_segment(
                    segments.len(),
                    start_frame,
                    end_frame,
                    start_timestamp,
                    metadata.duration_secs,
                    metadata,
                );
                segments.push(seg);
            }
            break;
        }
    }

    Ok(segments)
}

/// Build a Segment with computed complexity and scene change info.
fn build_segment(
    id: usize,
    start_frame: u64,
    end_frame: u64,
    start_timestamp: f64,
    end_timestamp: f64,
    metadata: &VideoMetadata,
) -> Segment {
    // Average complexity for this frame range
    let complexity_estimate = if !metadata.complexity_map.is_empty() {
        let start = (start_frame as usize).min(metadata.complexity_map.len());
        let end = ((end_frame + 1) as usize).min(metadata.complexity_map.len());
        if end > start {
            let sum: f32 = metadata.complexity_map[start..end].iter().sum();
            sum / (end - start) as f32
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Scene changes within this segment
    let contains_scene_changes: Vec<u64> = metadata
        .scene_changes
        .iter()
        .copied()
        .filter(|&sc| sc >= start_frame && sc <= end_frame)
        .collect();

    Segment {
        id,
        start_frame,
        end_frame,
        start_timestamp,
        end_timestamp,
        complexity_estimate,
        contains_scene_changes,
    }
}

/// Find the index of the keyframe position nearest to `target_frame`.
fn find_nearest_keyframe(keyframe_positions: &[u64], target_frame: u64) -> usize {
    match keyframe_positions.binary_search(&target_frame) {
        Ok(idx) => idx,
        Err(idx) => {
            if idx == 0 {
                0
            } else if idx >= keyframe_positions.len() {
                keyframe_positions.len() - 1
            } else {
                // Pick whichever keyframe is closer to target
                let before = keyframe_positions[idx - 1];
                let after = keyframe_positions[idx];
                if target_frame - before <= after - target_frame {
                    idx - 1
                } else {
                    idx
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_metadata(total_frames: u64, fps: f64, keyframes: Vec<u64>) -> VideoMetadata {
        let duration_secs = total_frames as f64 / fps;
        let keyframe_timestamps: Vec<f64> = keyframes.iter().map(|&f| f as f64 / fps).collect();
        VideoMetadata {
            duration_secs,
            width: 1920,
            height: 1080,
            fps,
            total_frames,
            codec_name: "h264".into(),
            keyframe_positions: keyframes,
            keyframe_timestamps,
            scene_changes: vec![],
            complexity_map: vec![0.5; total_frames as usize],
            gop_stats: vec![],
            profile: None,
            pix_fmt: None,
            audio_tracks: vec![],
            subtitle_tracks: vec![],
            chapters: vec![],
        }
    }

    #[test]
    fn test_find_nearest_keyframe() {
        let kfs = vec![0, 150, 300, 450, 600];
        assert_eq!(find_nearest_keyframe(&kfs, 0), 0);
        assert_eq!(find_nearest_keyframe(&kfs, 140), 1); // closer to 150
        assert_eq!(find_nearest_keyframe(&kfs, 160), 1); // closer to 150
        assert_eq!(find_nearest_keyframe(&kfs, 225), 1); // equidistant, picks earlier
        assert_eq!(find_nearest_keyframe(&kfs, 226), 2); // closer to 300
        assert_eq!(find_nearest_keyframe(&kfs, 1000), 4); // beyond last
    }

    #[test]
    fn test_create_segments_basic() {
        // 30fps, 900 frames = 30 seconds, keyframes every 150 frames (5s)
        let meta = mock_metadata(900, 30.0, vec![0, 150, 300, 450, 600, 750]);

        // Target 10s segments = 300 frames
        let segments = create_segments(&meta, 10.0).unwrap();
        assert!(!segments.is_empty());

        // All segments should start at keyframe boundaries
        for seg in &segments {
            assert!(
                meta.keyframe_positions.contains(&seg.start_frame),
                "Segment {} start_frame {} is not a keyframe",
                seg.id,
                seg.start_frame
            );
        }

        // Segments should cover the full video
        assert_eq!(segments.first().unwrap().start_frame, 0);
        assert_eq!(segments.last().unwrap().end_frame, 899);

        // No gaps between segments
        for i in 1..segments.len() {
            assert_eq!(
                segments[i].start_frame,
                segments[i - 1].end_frame + 1,
                "Gap between segment {} and {}",
                i - 1,
                i
            );
        }
    }

    #[test]
    fn test_create_segments_single_keyframe() {
        let meta = mock_metadata(300, 30.0, vec![0]);
        let segments = create_segments(&meta, 10.0).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start_frame, 0);
        assert_eq!(segments[0].end_frame, 299);
    }

    #[test]
    fn test_create_segments_with_scene_changes() {
        let mut meta = mock_metadata(900, 30.0, vec![0, 150, 300, 450, 600, 750]);
        meta.scene_changes = vec![200, 500];

        let segments = create_segments(&meta, 10.0).unwrap();

        // Find which segments contain the scene changes
        let seg_with_200 = segments.iter().find(|s| s.contains_scene_changes.contains(&200));
        let seg_with_500 = segments.iter().find(|s| s.contains_scene_changes.contains(&500));
        assert!(seg_with_200.is_some(), "Scene change at 200 should be in a segment");
        assert!(seg_with_500.is_some(), "Scene change at 500 should be in a segment");
    }

    #[test]
    fn test_no_frames_error() {
        let meta = mock_metadata(0, 30.0, vec![]);
        assert!(create_segments(&meta, 10.0).is_err());
    }
}
