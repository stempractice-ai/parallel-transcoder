//! OBS-websocket inspired cluster protocol.
//!
//! Messages use an OpCode + data pattern, similar to the obs-websocket protocol.
//! Wire format: `{ "op": number, "d": object }` serialized as JSON over WebSocket.

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

pub type NodeId = Uuid;
pub type JobId = Uuid;

/// OBS-websocket inspired OpCodes.
///
/// Wire format: `op` is the numeric discriminant (e.g. `50` for `StatusRequest`),
/// per the documented `{ "op": number, "d": object }` JSON shape. `serde_repr`
/// is required because the default `Serialize` derive ignores `#[repr(u8)]` and
/// would otherwise emit the variant *name* instead of the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum OpCode {
    // Handshake
    Hello = 0,
    Identify = 1,
    Identified = 2,

    // Election
    ElectionStart = 10,
    ElectionAlive = 11,
    ElectionVictory = 12,

    // Heartbeat
    Heartbeat = 20,
    HeartbeatAck = 21,

    // Job management
    JobSubmit = 30,
    JobAccepted = 31,
    JobProgress = 32,
    JobComplete = 33,
    JobFailed = 34,
    JobCancel = 35,
    AnalyzeRequest = 36,
    AnalyzeResult = 37,

    // Segment distribution
    SegmentAssign = 40,
    SegmentAssignAck = 41,
    SegmentComplete = 42,
    SegmentFailed = 43,

    // Cluster status
    StatusRequest = 50,
    StatusResponse = 51,

    // Events (server->client push)
    Event = 60,

    // Node lifecycle
    NodeLeave = 70,

    // Error
    Error = 255,
}

/// Top-level wire message (OBS-websocket pattern)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub op: OpCode,
    pub d: serde_json::Value,
}

impl Message {
    pub fn new<T: Serialize>(op: OpCode, data: &T) -> anyhow::Result<Self> {
        Ok(Self {
            op,
            d: serde_json::to_value(data)?,
        })
    }

    pub fn parse_data<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_value(self.d.clone())?)
    }
}

// --- Handshake payloads ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloData {
    pub cluster_name: String,
    pub protocol_version: u32,
    pub master_id: Option<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifyData {
    pub node_id: NodeId,
    pub listen_addr: String,
    pub capabilities: NodeCapabilities,
    pub event_subscriptions: u32, // bitmask
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifiedData {
    pub node_id: NodeId,
    pub master_id: Option<NodeId>,
    pub cluster_nodes: Vec<NodeInfo>,
}

// --- Node capabilities ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub cpu_cores: usize,
    pub available_memory_mb: u64,
    pub has_gpu: bool,
    pub gpu_encoder: Option<String>,
    pub hostname: String,
    pub os: String,
    pub max_concurrent_workers: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeLoad {
    pub active_workers: usize,
    pub cpu_usage_percent: f32,
    pub memory_used_mb: u64,
    pub segments_completed: u64,
    pub segments_failed: u64,
}

// --- Election payloads ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionStartData {
    pub candidate_id: NodeId,
    pub priority: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionAliveData {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionVictoryData {
    pub master_id: NodeId,
}

// --- Heartbeat payloads ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatData {
    pub node_id: NodeId,
    pub load: NodeLoad,
}

// --- Job payloads ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodingConfig {
    pub crf: u32,
    pub preset: String,
    pub encoder: String,
    pub format: String,
    pub fast_mode: bool,
    pub hw_decode: bool,
    /// "normal", "copy", "smart" or "smart-auto"; older senders omit it.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Bitrate tolerance for the smart modes.
    #[serde(default = "default_smart_tolerance")]
    pub smart_tolerance: f64,
}

fn default_mode() -> String {
    "normal".into()
}

fn default_smart_tolerance() -> f64 {
    0.3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSubmitData {
    pub job_id: JobId,
    pub input_filename: String,
    pub input_size_bytes: u64,
    pub config: EncodingConfig,
    /// SRT URL where the input file can be fetched
    pub srt_input_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobAcceptedData {
    pub job_id: JobId,
    pub total_segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgressData {
    pub job_id: JobId,
    pub completed_segments: usize,
    pub total_segments: usize,
    pub failed_segments: usize,
    pub phase: String,
}

// --- Analysis payloads ---

/// Asks the master to probe a source and recommend a mode and preset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeRequestData {
    pub request_id: Uuid,
    pub source_url: String,
    pub encoder: String,
    pub crf: u32,
    pub smart_tolerance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeResultData {
    pub request_id: Uuid,
    pub media: crate::media::MediaSummary,
    pub recommendation: crate::media::Recommendation,
}

// --- Segment payloads ---

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
    /// Stream-copy this segment's video instead of re-encoding it.
    #[serde(default)]
    pub copy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentAssignData {
    pub job_id: JobId,
    pub segment: SegmentDescriptor,
    /// SRT URL to fetch this segment's pre-split data
    pub srt_url: String,
    pub encoding_config: EncodingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentAssignAckData {
    pub job_id: JobId,
    pub segment_id: usize,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentResult {
    pub segment_id: usize,
    pub worker_id: usize,
    pub node_id: NodeId,
    pub frames_encoded: u64,
    pub output_size_bytes: u64,
    pub encoding_time_secs: f64,
    pub average_complexity: f32,
    pub scene_changes_detected: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentCompleteData {
    pub job_id: JobId,
    pub result: SegmentResult,
    /// SRT URL to fetch the encoded segment output
    pub srt_output_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFailedData {
    pub job_id: JobId,
    pub segment_id: usize,
    pub node_id: NodeId,
    pub error: String,
}

// --- Status payloads ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub capabilities: NodeCapabilities,
    pub load: NodeLoad,
    pub is_master: bool,
    pub last_heartbeat_ms: u64,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Joining,
    Active,
    Busy,
    Draining,
    Dead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Analyzing,
    Distributing,
    Processing,
    Assembling,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusInfo {
    pub job_id: JobId,
    pub state: JobState,
    pub total_segments: usize,
    pub completed_segments: usize,
    pub failed_segments: usize,
    pub assigned_nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponseData {
    pub master_id: Option<NodeId>,
    pub nodes: Vec<NodeInfo>,
    pub active_jobs: Vec<JobStatusInfo>,
}

// --- Event subscription bitmask (OBS pattern) ---

pub mod event_subs {
    pub const NONE: u32 = 0;
    pub const JOB_PROGRESS: u32 = 1 << 0;
    pub const SEGMENT_STATUS: u32 = 1 << 1;
    pub const NODE_STATUS: u32 = 1 << 2;
    pub const ELECTION: u32 = 1 << 3;
    pub const ALL: u32 = 0xFFFFFFFF;
}

// --- Error payload ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorData {
    pub code: u32,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_roundtrip_hello() {
        let hello = HelloData {
            cluster_name: "test-cluster".into(),
            protocol_version: 1,
            master_id: None,
        };
        let msg = Message::new(OpCode::Hello, &hello).unwrap();
        assert_eq!(msg.op, OpCode::Hello);

        let parsed: HelloData = msg.parse_data().unwrap();
        assert_eq!(parsed.cluster_name, "test-cluster");
        assert_eq!(parsed.protocol_version, 1);
        assert!(parsed.master_id.is_none());
    }

    #[test]
    fn test_message_roundtrip_identify() {
        let node_id = Uuid::new_v4();
        let identify = IdentifyData {
            node_id,
            listen_addr: "192.168.1.10:9000".into(),
            capabilities: NodeCapabilities {
                cpu_cores: 8,
                available_memory_mb: 16384,
                has_gpu: true,
                gpu_encoder: Some("h264_videotoolbox".into()),
                hostname: "mac-pro".into(),
                os: "macos".into(),
                max_concurrent_workers: 4,
            },
            event_subscriptions: event_subs::ALL,
        };
        let msg = Message::new(OpCode::Identify, &identify).unwrap();
        assert_eq!(msg.op, OpCode::Identify);

        let parsed: IdentifyData = msg.parse_data().unwrap();
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.capabilities.cpu_cores, 8);
        assert_eq!(parsed.event_subscriptions, event_subs::ALL);
    }

    #[test]
    fn test_message_roundtrip_heartbeat() {
        let node_id = Uuid::new_v4();
        let hb = HeartbeatData {
            node_id,
            load: NodeLoad {
                active_workers: 3,
                cpu_usage_percent: 75.5,
                memory_used_mb: 8192,
                segments_completed: 42,
                segments_failed: 1,
            },
        };
        let msg = Message::new(OpCode::Heartbeat, &hb).unwrap();
        let parsed: HeartbeatData = msg.parse_data().unwrap();
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.load.active_workers, 3);
        assert_eq!(parsed.load.segments_completed, 42);
    }

    #[test]
    fn test_message_roundtrip_segment_assign() {
        let job_id = Uuid::new_v4();
        let assign = SegmentAssignData {
            job_id,
            segment: SegmentDescriptor {
                id: 5,
                start_frame: 1000,
                end_frame: 2000,
                start_timestamp: 33.33,
                end_timestamp: 66.66,
                lookahead_frames: Some(30),
                complexity_estimate: 0.75,
                scene_changes: vec![1200, 1800],
                copy: false,
            },
            srt_url: "srt://192.168.1.1:9100?mode=caller".into(),
            encoding_config: EncodingConfig {
                crf: 23,
                preset: "medium".into(),
                encoder: "libx264".into(),
                format: "hls".into(),
                fast_mode: false,
                hw_decode: true,
                mode: "normal".into(),
                smart_tolerance: 0.3,
            },
        };
        let msg = Message::new(OpCode::SegmentAssign, &assign).unwrap();
        let parsed: SegmentAssignData = msg.parse_data().unwrap();
        assert_eq!(parsed.segment.id, 5);
        assert_eq!(parsed.segment.scene_changes.len(), 2);
        assert_eq!(parsed.srt_url, "srt://192.168.1.1:9100?mode=caller");
    }

    #[test]
    fn test_message_json_wire_format() {
        let err = ErrorData {
            code: 404,
            message: "Node not found".into(),
        };
        let msg = Message::new(OpCode::Error, &err).unwrap();
        let json = serde_json::to_string(&msg).unwrap();

        // Verify it contains "op" and "d" keys
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("op").is_some());
        assert!(value.get("d").is_some());

        // `op` must serialize as the numeric discriminant — non-Rust clients
        // (the JS web server) send and expect numbers per the documented
        // wire format. If this regresses, JS↔Rust messages stop parsing.
        assert_eq!(value.get("op").and_then(|v| v.as_u64()), Some(255));

        // Round-trip through JSON
        let msg2: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(msg2.op, OpCode::Error);
        let parsed: ErrorData = msg2.parse_data().unwrap();
        assert_eq!(parsed.code, 404);
        assert_eq!(parsed.message, "Node not found");
    }

    #[test]
    fn test_opcode_numeric_wire_format() {
        // Numeric on the wire (matches JS clients and protocol docs).
        let json = serde_json::to_string(&OpCode::StatusRequest).unwrap();
        assert_eq!(json, "50");
        let json = serde_json::to_string(&OpCode::Heartbeat).unwrap();
        assert_eq!(json, "20");

        // Deserialize from numeric form (what the JS web server sends).
        let m: Message = serde_json::from_str(r#"{"op":50,"d":{}}"#).unwrap();
        assert_eq!(m.op, OpCode::StatusRequest);

        assert_eq!(serde_json::to_string(&OpCode::AnalyzeRequest).unwrap(), "36");
        assert_eq!(serde_json::to_string(&OpCode::AnalyzeResult).unwrap(), "37");
    }

    #[test]
    fn test_encoding_config_defaults_mode_when_absent() {
        let json = r#"{"crf":23,"preset":"medium","encoder":"libx264","format":"mp4","fast_mode":false,"hw_decode":false}"#;
        let c: EncodingConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.mode, "normal");
        assert_eq!(c.smart_tolerance, 0.3);
    }

    #[test]
    fn test_segment_descriptor_defaults_copy_false() {
        let json = r#"{"id":0,"start_frame":0,"end_frame":60,"start_timestamp":0.0,"end_timestamp":2.0,"complexity_estimate":0.5,"scene_changes":[]}"#;
        let s: SegmentDescriptor = serde_json::from_str(json).unwrap();
        assert!(!s.copy);
    }

    #[test]
    fn test_message_roundtrip_election() {
        let candidate_id = Uuid::new_v4();
        let start = ElectionStartData {
            candidate_id,
            priority: 12345,
        };
        let msg = Message::new(OpCode::ElectionStart, &start).unwrap();
        assert_eq!(msg.op, OpCode::ElectionStart);

        let parsed: ElectionStartData = msg.parse_data().unwrap();
        assert_eq!(parsed.candidate_id, candidate_id);
        assert_eq!(parsed.priority, 12345);
    }

    #[test]
    fn test_message_roundtrip_status_response() {
        let node_id = Uuid::new_v4();
        let status = StatusResponseData {
            master_id: Some(node_id),
            nodes: vec![NodeInfo {
                node_id,
                address: "10.0.0.1:9000".into(),
                capabilities: NodeCapabilities {
                    cpu_cores: 16,
                    available_memory_mb: 65536,
                    has_gpu: false,
                    gpu_encoder: None,
                    hostname: "worker-1".into(),
                    os: "linux".into(),
                    max_concurrent_workers: 8,
                },
                load: NodeLoad::default(),
                is_master: true,
                last_heartbeat_ms: 0,
                status: NodeStatus::Active,
            }],
            active_jobs: vec![],
        };
        let msg = Message::new(OpCode::StatusResponse, &status).unwrap();
        let parsed: StatusResponseData = msg.parse_data().unwrap();
        assert_eq!(parsed.nodes.len(), 1);
        assert_eq!(parsed.nodes[0].capabilities.cpu_cores, 16);
    }

    #[test]
    fn test_default_node_load() {
        let load = NodeLoad::default();
        assert_eq!(load.active_workers, 0);
        assert_eq!(load.cpu_usage_percent, 0.0);
        assert_eq!(load.memory_used_mb, 0);
        assert_eq!(load.segments_completed, 0);
        assert_eq!(load.segments_failed, 0);
    }

    #[test]
    fn test_event_subscription_bitmask() {
        let subs = event_subs::JOB_PROGRESS | event_subs::NODE_STATUS;
        assert_ne!(subs & event_subs::JOB_PROGRESS, 0);
        assert_eq!(subs & event_subs::SEGMENT_STATUS, 0);
        assert_ne!(subs & event_subs::NODE_STATUS, 0);
        assert_eq!(subs & event_subs::ELECTION, 0);

        assert_ne!(event_subs::ALL & event_subs::JOB_PROGRESS, 0);
        assert_eq!(event_subs::NONE & event_subs::JOB_PROGRESS, 0);
    }
}
