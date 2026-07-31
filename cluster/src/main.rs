//! `transcoder-node` — Distributed cluster daemon for parallel video transcoding.
//!
//! Each machine in the cluster runs this binary. It participates in leader election
//! (Bully algorithm), and either acts as master (accepting jobs, distributing segments)
//! or worker (transcoding assigned segments locally).
//!
//! # Usage
//!
//! ```bash
//! # Bootstrap a new cluster (first node becomes master):
//! transcoder-node --listen 0.0.0.0:9900
//!
//! # Join an existing cluster as a worker:
//! transcoder-node --listen 0.0.0.0:9901 --join 192.168.1.10:9900
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

// Import from the crate library.
// NOTE: lib.rs must export these modules. If election/node/scheduler are still
// commented out in lib.rs, uncomment them before building.
use transcoder_cluster::election::ElectionManager;
use transcoder_cluster::node::NodeManager;
use transcoder_cluster::object_store::{is_s3_uri, ObjectStore};
use transcoder_cluster::protocol::*;
use transcoder_cluster::scheduler::Scheduler;
use transcoder_cluster::srt::{SrtMode, SrtServer};
use transcoder_cluster::transport::{PeerMessage, Transport};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Distributed video transcoding cluster node.
///
/// Runs on each machine in the cluster. Participates in leader election,
/// accepts jobs (as master), or transcodes segments (as worker).
#[derive(Parser, Debug)]
#[command(name = "transcoder-node", version, about)]
struct Cli {
    /// Listen address for cluster communication.
    #[arg(short, long, default_value = "0.0.0.0:9900")]
    listen: String,

    /// Join an existing cluster at this address.
    #[arg(short, long)]
    join: Option<String>,

    /// Human-readable node name (defaults to hostname).
    #[arg(short, long)]
    name: Option<String>,

    /// Base port for SRT data streams.
    #[arg(long, default_value_t = 9910)]
    srt_base_port: u16,

    /// Path to the transcoder-worker binary.
    #[arg(long, default_value = "./bin/transcoder-worker")]
    worker_binary: PathBuf,

    /// Path to FFmpeg shared libraries.
    #[arg(long, default_value = "./lib/")]
    lib_dir: PathBuf,

    /// Enable verbose (debug-level) logging.
    #[arg(short, long)]
    verbose: bool,

    /// Kubernetes mode: pod-0 is deterministic master (no bully election).
    ///
    /// Reads `POD_ORDINAL` from env (set via StatefulSet downward API). Ordinal
    /// 0 bootstraps as master; all others treat pod-0 as the leader from the
    /// start. K8s handles restart and failover, so bully election would just
    /// fight the control loop. Pairs with `--join <headless-svc>-0.<svc>:9900`
    /// on non-zero ordinals.
    #[arg(long)]
    k8s_mode: bool,

    /// Object-storage endpoint (e.g. `http://minio.default.svc:9000`).
    ///
    /// When set, segment data flows through the bucket instead of SRT. The
    /// SRT data plane is still used on desktop / LAN clusters.
    #[arg(long)]
    object_store_url: Option<String>,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CLUSTER_NAME: &str = "transcoder-cluster";
const PROTOCOL_VERSION: u32 = 1;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const ELECTION_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const SCHEDULE_INTERVAL: Duration = Duration::from_secs(1);

const ELECTION_TIMEOUT: Duration = Duration::from_secs(3);
const VICTORY_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

/// Per-job metadata tracked by the master.
#[allow(dead_code)]
struct JobContext {
    config: EncodingConfig,
    input_filename: String,
    segments: Vec<SegmentDescriptor>,
    /// segment_id -> object-store URI of its encoded output, filled in as
    /// SegmentComplete messages arrive. Used to assemble the final output
    /// once every segment is done.
    output_uris: HashMap<usize, String>,
}

/// Top-level application state shared across the event loop.
struct App {
    node_id: NodeId,
    listen_addr: SocketAddr,
    capabilities: NodeCapabilities,
    transport: Transport,
    election: ElectionManager,
    nodes: NodeManager,
    scheduler: Scheduler,
    srt: SrtServer,
    /// Segment descriptors kept by the master for reassignment.
    job_contexts: HashMap<JobId, JobContext>,
    /// Active local worker child processes (segment_id → cancel sender).
    local_workers: HashMap<usize, mpsc::Sender<()>>,
    /// Pending worker results waiting to be relayed to the master.
    pending_results: HashMap<usize, (mpsc::UnboundedReceiver<ResultMessage>, SocketAddr)>,
    /// Final-output assembly tasks in flight (job_id -> result channel).
    pending_assembly: HashMap<JobId, mpsc::UnboundedReceiver<AssemblyResult>>,
    /// CLI configuration.
    worker_binary: PathBuf,
    lib_dir: PathBuf,
    /// Optional object-storage client (K8s mode). When present, segment
    /// transfer goes through the bucket instead of SRT.
    object_store: Option<ObjectStore>,
}

impl App {
    fn new(
        node_id: NodeId,
        listen_addr: SocketAddr,
        capabilities: NodeCapabilities,
        transport: Transport,
        worker_binary: PathBuf,
        lib_dir: PathBuf,
        srt_base_port: u16,
        object_store: Option<ObjectStore>,
    ) -> Self {
        Self {
            node_id,
            listen_addr,
            capabilities,
            transport,
            election: ElectionManager::new(node_id, ELECTION_TIMEOUT, VICTORY_TIMEOUT),
            nodes: NodeManager::new(node_id),
            scheduler: Scheduler::new(),
            srt: SrtServer::new(srt_base_port),
            job_contexts: HashMap::new(),
            local_workers: HashMap::new(),
            pending_results: HashMap::new(),
            pending_assembly: HashMap::new(),
            worker_binary,
            lib_dir,
            object_store,
        }
    }

    /// Returns true if this node is the cluster master.
    fn is_master(&self) -> bool {
        self.election.is_leader()
    }

    // --------------------------------------------------------------------
    // Self-registration: register ourselves in the node table.
    // --------------------------------------------------------------------

    fn register_self(&self) {
        let info = NodeInfo {
            node_id: self.node_id,
            address: self.listen_addr.to_string(),
            capabilities: self.capabilities.clone(),
            load: NodeLoad::default(),
            is_master: self.is_master(),
            last_heartbeat_ms: 0,
            status: NodeStatus::Active,
        };
        self.nodes.register_node(info);
    }

    // ====================================================================
    // Message dispatch
    // ====================================================================

    async fn handle_message(&mut self, peer: PeerMessage) {
        let PeerMessage {
            peer_addr,
            message,
            ..
        } = peer;

        match message.op {
            // -- Handshake --
            OpCode::Hello => self.handle_hello(peer_addr),
            OpCode::Identify => self.handle_identify(peer_addr, &message),
            OpCode::Identified => self.handle_identified(peer_addr, &message),

            // -- Election --
            OpCode::ElectionStart => self.handle_election_start(peer_addr, &message),
            OpCode::ElectionAlive => self.handle_election_alive(&message),
            OpCode::ElectionVictory => self.handle_election_victory(&message),

            // -- Heartbeat --
            OpCode::Heartbeat => self.handle_heartbeat(&message),
            OpCode::HeartbeatAck => { /* acknowledged */ }

            // -- Jobs --
            OpCode::JobSubmit => self.handle_job_submit(peer_addr, &message).await,

            // -- Segments --
            OpCode::SegmentAssign => self.handle_segment_assign(peer_addr, &message).await,
            OpCode::SegmentComplete => self.handle_segment_complete(&message),
            OpCode::SegmentFailed => self.handle_segment_failed(&message),

            // -- Status --
            OpCode::StatusRequest => self.handle_status_request(peer_addr),

            // -- Lifecycle --
            OpCode::NodeLeave => self.handle_node_leave(&message),

            _ => {
                debug!(op = ?message.op, "Unhandled opcode");
            }
        }
    }

    // ====================================================================
    // Handshake
    // ====================================================================

    /// Respond to a Hello from a connecting peer by sending our Identify.
    fn handle_hello(&self, peer_addr: SocketAddr) {
        info!(%peer_addr, "Received Hello, sending Identify");
        let identify = IdentifyData {
            node_id: self.node_id,
            listen_addr: self.listen_addr.to_string(),
            capabilities: self.capabilities.clone(),
            event_subscriptions: event_subs::ALL,
        };
        if let Ok(msg) = Message::new(OpCode::Identify, &identify) {
            let _ = self.transport.send_to(&peer_addr, msg);
        }
    }

    /// Process a peer's Identify: register them and send Identified back.
    fn handle_identify(&mut self, peer_addr: SocketAddr, msg: &Message) {
        let Ok(data) = msg.parse_data::<IdentifyData>() else {
            warn!("Failed to parse Identify payload");
            return;
        };
        info!(
            node_id = %data.node_id,
            addr = %data.listen_addr,
            hostname = %data.capabilities.hostname,
            cores = data.capabilities.cpu_cores,
            "Peer identified"
        );

        // Associate the node_id with this transport address.
        self.transport.set_peer_node_id(&peer_addr, data.node_id);

        // Register the peer in the node table.
        let info = NodeInfo {
            node_id: data.node_id,
            address: data.listen_addr,
            capabilities: data.capabilities,
            load: NodeLoad::default(),
            is_master: false,
            last_heartbeat_ms: 0,
            status: NodeStatus::Active,
        };
        self.nodes.register_node(info);

        // Send our cluster state back.
        let identified = IdentifiedData {
            node_id: self.node_id,
            master_id: self.election.current_leader(),
            cluster_nodes: self.nodes.all_nodes(),
        };
        if let Ok(resp) = Message::new(OpCode::Identified, &identified) {
            let _ = self.transport.send_to(&peer_addr, resp);
        }
    }

    /// Process an Identified response: learn about the cluster.
    fn handle_identified(&mut self, peer_addr: SocketAddr, msg: &Message) {
        let Ok(data) = msg.parse_data::<IdentifiedData>() else {
            warn!("Failed to parse Identified payload");
            return;
        };
        info!(
            peer = %data.node_id,
            master = ?data.master_id,
            cluster_size = data.cluster_nodes.len(),
            "Handshake complete"
        );

        // Associate this connection with the peer's node_id. Identified is
        // sent in reply to our own Identify, and — unlike handle_identify —
        // nothing else ever tags this side of the connection, so without
        // this the scheduler can never find a live peer address for anyone
        // who connected *to* us (i.e. every worker, since they always join
        // us, never the other way around).
        self.transport.set_peer_node_id(&peer_addr, data.node_id);

        // Register all advertised cluster nodes.
        for node in data.cluster_nodes {
            if node.node_id != self.node_id {
                self.nodes.register_node(node);
            }
        }

        // Accept their master if we don't have one yet.
        if let Some(master_id) = data.master_id {
            if self.election.current_leader().is_none() {
                let victory = ElectionVictoryData { master_id };
                self.election.handle_victory(&victory);
                info!(%master_id, "Accepted existing cluster master");
            }
        }
    }

    // ====================================================================
    // Election
    // ====================================================================

    fn handle_election_start(&mut self, peer_addr: SocketAddr, msg: &Message) {
        let Ok(data) = msg.parse_data::<ElectionStartData>() else {
            return;
        };

        let was_leader = self.election.is_leader();

        if let Some(alive_msg) = self.election.handle_election_start(&data) {
            // Send ElectionAlive back to the lower-priority candidate.
            let _ = self.transport.send_to(&peer_addr, alive_msg);

            // If we were already leader, just re-announce victory instead of
            // restarting the full election cycle.
            if was_leader {
                let victory = ElectionVictoryData {
                    master_id: self.node_id,
                };
                if let Ok(msg) = Message::new(OpCode::ElectionVictory, &victory) {
                    self.transport.broadcast(&msg, None);
                }
            } else {
                // We're now a Candidate (set by handle_election_start).
                // Broadcast our own ElectionStart so other nodes know.
                let start_msg = self.election.start_election();
                self.transport.broadcast(&start_msg, Some(&peer_addr));
            }
        }
    }

    fn handle_election_alive(&mut self, msg: &Message) {
        let Ok(data) = msg.parse_data::<ElectionAliveData>() else {
            return;
        };
        self.election.handle_alive(&data);
    }

    fn handle_election_victory(&mut self, msg: &Message) {
        let Ok(data) = msg.parse_data::<ElectionVictoryData>() else {
            return;
        };
        info!(master_id = %data.master_id, "New master elected");
        self.election.handle_victory(&data);
    }

    // ====================================================================
    // Heartbeat
    // ====================================================================

    fn handle_heartbeat(&self, msg: &Message) {
        let Ok(data) = msg.parse_data::<HeartbeatData>() else {
            return;
        };
        self.nodes.update_heartbeat(&data.node_id, data.load);
    }

    fn send_heartbeat(&self) {
        let hb = HeartbeatData {
            node_id: self.node_id,
            load: NodeLoad {
                active_workers: self.local_workers.len(),
                cpu_usage_percent: 0.0,
                memory_used_mb: 0,
                segments_completed: 0,
                segments_failed: 0,
            },
        };
        if let Ok(msg) = Message::new(OpCode::Heartbeat, &hb) {
            self.transport.broadcast(&msg, None);
        }
    }

    // ====================================================================
    // Job management (master only)
    // ====================================================================

    async fn handle_job_submit(&mut self, peer_addr: SocketAddr, msg: &Message) {
        if !self.is_master() {
            warn!("Received JobSubmit but not master — rejecting");
            let err = ErrorData {
                code: 403,
                message: "Not the master node".into(),
            };
            if let Ok(resp) = Message::new(OpCode::Error, &err) {
                let _ = self.transport.send_to(&peer_addr, resp);
            }
            return;
        }

        let Ok(data) = msg.parse_data::<JobSubmitData>() else {
            warn!("Failed to parse JobSubmit payload");
            return;
        };

        info!(
            job_id = %data.job_id,
            input = %data.input_filename,
            size_mb = data.input_size_bytes / (1024 * 1024),
            "Job submitted"
        );

        // In K8s mode the submitter uploads the source video to the object
        // store first and points us at it via srt_input_url (an s3:// URI
        // despite the field name — same field, no protocol/version bump).
        // We download it, cut real segments with ffmpeg, and upload each
        // segment back to the bucket at the key the scheduler already
        // expects (jobs/{job_id}/input/seg_{id}.ts). Without this step
        // there's nothing at that key and every segment assignment fails
        // with NoSuchKey once a worker tries to fetch it.
        let segments = match (&self.object_store, &data.srt_input_url) {
            (Some(store), Some(uri)) if is_s3_uri(uri) => {
                match segment_and_upload(store, data.job_id, uri).await {
                    Ok(segments) => segments,
                    Err(e) => {
                        warn!(job_id = %data.job_id, error = ?e, "Failed to segment source video");
                        let err = ErrorData {
                            code: 500,
                            message: format!("Failed to segment source video: {:?}", e),
                        };
                        if let Ok(resp) = Message::new(OpCode::Error, &err) {
                            let _ = self.transport.send_to(&peer_addr, resp);
                        }
                        return;
                    }
                }
            }
            _ => analyze_video(&data),
        };
        let total_segments = segments.len();

        // Store job context for later segment distribution.
        self.job_contexts.insert(
            data.job_id,
            JobContext {
                config: data.config,
                input_filename: data.input_filename,
                segments: segments.clone(),
                output_uris: HashMap::new(),
            },
        );

        // Enqueue segments for scheduling.
        self.scheduler.add_job(data.job_id, segments);

        // Acknowledge to the submitter.
        let accepted = JobAcceptedData {
            job_id: data.job_id,
            total_segments,
        };
        if let Ok(resp) = Message::new(OpCode::JobAccepted, &accepted) {
            let _ = self.transport.send_to(&peer_addr, resp);
        }

        info!(
            job_id = %data.job_id,
            total_segments,
            "Job accepted, segments queued"
        );
    }

    // ====================================================================
    // Segment scheduling (master → workers)
    // ====================================================================

    fn run_scheduler(&mut self) {
        if !self.is_master() {
            return;
        }

        // NodeManager tracks nodes it has merely heard *about* (e.g. gossiped
        // via another peer's Identified payload) the same way as nodes it has
        // a live transport connection to — both look "Active" with a fresh
        // heartbeat. Only nodes we can actually reach over the transport are
        // eligible for scheduling, or SegmentAssign has nowhere to go and the
        // segment sits stuck forever. This also excludes the master's own
        // node_id: there is no local-execution path for segments, only
        // connected workers can ever run one.
        let active: Vec<_> = self
            .nodes
            .active_nodes()
            .into_iter()
            .filter(|n| self.transport.find_peer_addr(&n.node_id).is_some())
            .collect();
        if active.is_empty() {
            return;
        }

        let assignments = self.scheduler.schedule(&active);
        for assignment in assignments {
            let node_id = assignment.node_id;
            let job_id = assignment.job_id;
            let segment = assignment.segment;
            let ctx = match self.job_contexts.get(&job_id) {
                Some(c) => c,
                None => continue,
            };

            // In K8s mode, the master writes the segment input into the
            // bucket and hands the worker an s3:// URI. In LAN/desktop mode
            // we keep the existing SRT data plane.
            let transfer_url = if let Some(store) = &self.object_store {
                store.build_uri(&format!("jobs/{}/input/seg_{}.ts", job_id, segment.id))
            } else {
                let srt_port = self.srt.allocate_port();
                SrtServer::build_url(
                    &self.listen_addr.ip().to_string(),
                    srt_port,
                    SrtMode::Caller,
                )
            };

            let assign_data = SegmentAssignData {
                job_id,
                segment: segment.clone(),
                srt_url: transfer_url,
                encoding_config: ctx.config.clone(),
            };

            let sent = if let Ok(msg) = Message::new(OpCode::SegmentAssign, &assign_data) {
                match self.transport.find_peer_addr(&node_id) {
                    Some(addr) => match self.transport.send_to(&addr, msg) {
                        Ok(()) => {
                            debug!(
                                %job_id, segment_id = segment.id, %node_id,
                                "Segment assigned"
                            );
                            true
                        }
                        Err(e) => {
                            warn!(
                                %node_id, segment_id = segment.id,
                                "Failed to send SegmentAssign: {}", e
                            );
                            false
                        }
                    },
                    None => {
                        warn!(%node_id, segment_id = segment.id, "No peer address, cannot assign segment");
                        false
                    }
                }
            } else {
                false
            };

            // The connection may have dropped between scheduling and sending
            // (or the node was never actually reachable). Requeue rather than
            // dropping the segment on the floor, so the next scheduling tick
            // retries against whichever nodes are genuinely connected.
            if !sent {
                self.scheduler.requeue_segment(&job_id, segment);
            }
        }
    }

    // ====================================================================
    // Segment processing (worker side)
    // ====================================================================

    async fn handle_segment_assign(&mut self, peer_addr: SocketAddr, msg: &Message) {
        let Ok(data) = msg.parse_data::<SegmentAssignData>() else {
            warn!("Failed to parse SegmentAssign payload");
            return;
        };

        info!(
            job_id = %data.job_id,
            segment_id = data.segment.id,
            time_range = format!("{:.1}s–{:.1}s", data.segment.start_timestamp, data.segment.end_timestamp),
            "Received segment assignment"
        );

        // Acknowledge the assignment.
        let ack = SegmentAssignAckData {
            job_id: data.job_id,
            segment_id: data.segment.id,
            node_id: self.node_id,
        };
        if let Ok(ack_msg) = Message::new(OpCode::SegmentAssignAck, &ack) {
            let _ = self.transport.send_to(&peer_addr, ack_msg);
        }

        // Track the local worker.
        let (cancel_tx, _cancel_rx) = mpsc::channel::<()>(1);
        self.local_workers.insert(data.segment.id, cancel_tx);

        // Spawn the worker process in a background task.
        let node_id = self.node_id;
        let worker_binary = self.worker_binary.clone();
        let lib_dir = self.lib_dir.clone();
        let listen_addr = self.listen_addr;
        let srt_port = self.srt.allocate_port();
        let object_store = self.object_store.clone();

        // Channel for the spawned task to send results back to the event loop.
        // We don't have access to the transport from a spawned task, so we use
        // a channel and process the result on the next iteration.
        let (result_tx, result_rx) = mpsc::unbounded_channel::<ResultMessage>();

        let job_id = data.job_id;
        let segment_id = data.segment.id;

        tokio::spawn(async move {
            let outcome = spawn_worker(
                &worker_binary,
                &lib_dir,
                &data,
                node_id,
                listen_addr,
                srt_port,
                object_store.as_ref(),
            )
            .await;

            let _ = result_tx.send(ResultMessage {
                job_id,
                segment_id,
                node_id,
                srt_port,
                output_uri: outcome.as_ref().ok().and_then(|r| r.output_uri.clone()),
                outcome: outcome.map(|r| r.result),
            });
        });

        // Store the result receiver so we can poll it from the event loop.
        // We can't use the transport from a spawned task, so results are
        // relayed via poll_worker_results() on each schedule tick.
        self.pending_results
            .insert(segment_id, (result_rx, peer_addr));
    }

    /// Poll pending worker results and relay them to the master.
    fn poll_worker_results(&mut self) {
        let mut completed_keys = Vec::new();

        for (&key, (rx, master_addr)) in &mut self.pending_results {
            match rx.try_recv() {
                Ok(result_msg) => {
                    self.local_workers.remove(&result_msg.segment_id);

                    match result_msg.outcome {
                        Ok(seg_result) => {
                            // If the worker uploaded to S3, carry that URI;
                            // otherwise fall back to an SRT listener URL
                            // pointing at this node.
                            let srt_output_url = result_msg.output_uri.unwrap_or_else(|| {
                                SrtServer::build_url(
                                    &self.listen_addr.ip().to_string(),
                                    result_msg.srt_port,
                                    SrtMode::Listener,
                                )
                            });
                            let complete = SegmentCompleteData {
                                job_id: result_msg.job_id,
                                result: seg_result,
                                srt_output_url,
                            };
                            if let Ok(msg) = Message::new(OpCode::SegmentComplete, &complete) {
                                let _ = self.transport.send_to(master_addr, msg);
                            }
                        }
                        Err(e) => {
                            let failed = SegmentFailedData {
                                job_id: result_msg.job_id,
                                segment_id: result_msg.segment_id,
                                node_id: result_msg.node_id,
                                error: format!("{:#}", e),
                            };
                            if let Ok(msg) = Message::new(OpCode::SegmentFailed, &failed) {
                                let _ = self.transport.send_to(master_addr, msg);
                            }
                        }
                    }

                    completed_keys.push(key);
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    completed_keys.push(key);
                }
            }
        }

        for key in completed_keys {
            self.pending_results.remove(&key);
        }
    }

    // ====================================================================
    // Segment results (master side)
    // ====================================================================

    fn handle_segment_complete(&mut self, msg: &Message) {
        if !self.is_master() {
            return;
        }

        let Ok(data) = msg.parse_data::<SegmentCompleteData>() else {
            warn!("Failed to parse SegmentComplete payload");
            return;
        };

        info!(
            job_id = %data.job_id,
            segment_id = data.result.segment_id,
            node = %data.result.node_id,
            time = format!("{:.1}s", data.result.encoding_time_secs),
            size_kb = data.result.output_size_bytes / 1024,
            "Segment completed"
        );

        let job_id = data.job_id;
        if is_s3_uri(&data.srt_output_url) {
            if let Some(ctx) = self.job_contexts.get_mut(&job_id) {
                ctx.output_uris
                    .insert(data.result.segment_id, data.srt_output_url.clone());
            }
        }
        self.scheduler.mark_complete(&job_id, data.result);
        self.check_job_completion(job_id);
    }

    fn handle_segment_failed(&mut self, msg: &Message) {
        if !self.is_master() {
            return;
        }

        let Ok(data) = msg.parse_data::<SegmentFailedData>() else {
            warn!("Failed to parse SegmentFailed payload");
            return;
        };

        warn!(
            job_id = %data.job_id,
            segment_id = data.segment_id,
            node = %data.node_id,
            error = %data.error,
            "Segment failed"
        );

        let job_id = data.job_id;
        self.scheduler.mark_failed(&job_id, data.segment_id, data.error);
        self.check_job_completion(job_id);
    }

    /// Check if a job has finished and broadcast the final status.
    fn check_job_completion(&mut self, job_id: JobId) {
        let Some((completed, total, failed)) = self.scheduler.job_progress(&job_id) else {
            return;
        };

        if !self.scheduler.is_job_complete(&job_id) {
            // Broadcast progress.
            let progress = JobProgressData {
                job_id,
                completed_segments: completed,
                total_segments: total,
                failed_segments: failed,
                phase: "processing".into(),
            };
            if let Ok(msg) = Message::new(OpCode::JobProgress, &progress) {
                self.transport.broadcast(&msg, None);
            }
            return;
        }

        // Job is done.
        if self.scheduler.has_failures(&job_id) {
            warn!(%job_id, completed, total, failed, "Job finished with failures");
            let fail_data = serde_json::json!({
                "job_id": job_id,
                "error": format!("{} of {} segments failed", failed, total),
            });
            if let Ok(msg) = Message::new(OpCode::JobFailed, &fail_data) {
                self.transport.broadcast(&msg, None);
            }
            self.job_contexts.remove(&job_id);
            return;
        }

        let ctx = self.job_contexts.remove(&job_id);
        let uris: Vec<(usize, String)> = ctx
            .as_ref()
            .map(|c| c.output_uris.clone().into_iter().collect())
            .unwrap_or_default();

        // If every segment's encoded output landed in the object store,
        // assemble the final file before telling the submitter the job is
        // done. Without an object store (SRT-only deployments) there's
        // nowhere shared to assemble into, so fall back to the old
        // segments-only completion signal.
        if let (Some(store), Some(ctx)) = (self.object_store.clone(), ctx.as_ref()) {
            if uris.len() == total {
                info!(%job_id, completed, total, "Job segments complete, assembling final output");
                let (tx, rx) = mpsc::unbounded_channel();
                self.pending_assembly.insert(job_id, rx);
                let format = ctx.config.format.clone();
                tokio::spawn(async move {
                    let outcome = assemble_output(&store, job_id, &format, uris).await;
                    let _ = tx.send(AssemblyResult {
                        total_segments: total,
                        outcome,
                    });
                });
                return;
            }
        }

        info!(%job_id, completed, total, "Job completed successfully");
        let complete_data = serde_json::json!({
            "job_id": job_id,
            "total_segments": total,
            "output_uri": null,
        });
        if let Ok(msg) = Message::new(OpCode::JobComplete, &complete_data) {
            self.transport.broadcast(&msg, None);
        }
    }

    /// Poll in-flight final-output assembly tasks and broadcast the result
    /// once each finishes.
    fn poll_assembly_results(&mut self) {
        let mut done = Vec::new();

        for (&job_id, rx) in &mut self.pending_assembly {
            match rx.try_recv() {
                Ok(result) => {
                    match result.outcome {
                        Ok(output_uri) => {
                            info!(%job_id, %output_uri, "Job completed successfully");
                            let complete_data = serde_json::json!({
                                "job_id": job_id,
                                "total_segments": result.total_segments,
                                "output_uri": output_uri,
                            });
                            if let Ok(msg) = Message::new(OpCode::JobComplete, &complete_data) {
                                self.transport.broadcast(&msg, None);
                            }
                        }
                        Err(e) => {
                            warn!(%job_id, error = ?e, "Failed to assemble final output");
                            let fail_data = serde_json::json!({
                                "job_id": job_id,
                                "error": format!("Failed to assemble final output: {:?}", e),
                            });
                            if let Ok(msg) = Message::new(OpCode::JobFailed, &fail_data) {
                                self.transport.broadcast(&msg, None);
                            }
                        }
                    }
                    done.push(job_id);
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => done.push(job_id),
            }
        }

        for job_id in done {
            self.pending_assembly.remove(&job_id);
        }
    }

    // ====================================================================
    // Status
    // ====================================================================

    fn handle_status_request(&self, peer_addr: SocketAddr) {
        let mut job_infos = Vec::new();
        for (&job_id, _ctx) in &self.job_contexts {
            let (completed, total, failed) = self.scheduler.job_progress(&job_id).unwrap_or((0, 0, 0));
            let state = if self.scheduler.is_job_complete(&job_id) {
                if self.scheduler.has_failures(&job_id) {
                    JobState::Failed
                } else {
                    JobState::Complete
                }
            } else {
                JobState::Processing
            };
            job_infos.push(JobStatusInfo {
                job_id,
                state,
                total_segments: total,
                completed_segments: completed,
                failed_segments: failed,
                assigned_nodes: self.scheduler.job_assigned_nodes(&job_id),
            });
        }

        let status = StatusResponseData {
            master_id: self.election.current_leader(),
            nodes: self.nodes.all_nodes(),
            active_jobs: job_infos,
        };
        if let Ok(msg) = Message::new(OpCode::StatusResponse, &status) {
            let _ = self.transport.send_to(&peer_addr, msg);
        }
    }

    // ====================================================================
    // Node leave / health checks / election timeout
    // ====================================================================

    fn handle_node_leave(&mut self, msg: &Message) {
        #[derive(serde::Deserialize)]
        struct NodeLeaveData {
            node_id: NodeId,
        }

        let Ok(data) = msg.parse_data::<NodeLeaveData>() else {
            return;
        };
        info!(node_id = %data.node_id, "Node leaving cluster");
        self.nodes.remove_node(&data.node_id);

        if self.is_master() {
            let reassigned = self.scheduler.reassign_from_node(&data.node_id);
            if reassigned > 0 {
                info!(count = reassigned, "Reassigned segments from departing node");
            }
        }

        // If the departing node was master, start an election.
        if self.election.current_leader() == Some(data.node_id) {
            warn!("Master left the cluster, starting election");
            self.election.reset();
            let start_msg = self.election.start_election();
            self.transport.broadcast(&start_msg, None);
        }
    }

    fn check_health(&mut self) {
        let dead = self.nodes.check_dead_nodes();
        for dead_id in &dead {
            warn!(node_id = %dead_id, "Detected dead node");

            if self.is_master() {
                let reassigned = self.scheduler.reassign_from_node(dead_id);
                if reassigned > 0 {
                    info!(
                        node_id = %dead_id,
                        count = reassigned,
                        "Reassigned segments from dead node"
                    );
                }
            }

            if self.election.current_leader() == Some(*dead_id) {
                warn!("Dead node was master, starting election");
                self.election.reset();
                let start_msg = self.election.start_election();
                self.transport.broadcast(&start_msg, None);
            }
        }
    }

    fn check_election(&mut self) {
        if let Some(msg) = self.election.check_timeout() {
            self.transport.broadcast(&msg, None);
        }
    }

    fn send_leave(&self) {
        let leave = serde_json::json!({ "node_id": self.node_id });
        if let Ok(msg) = Message::new(OpCode::NodeLeave, &leave) {
            self.transport.broadcast(&msg, None);
        }
    }
}

// ---------------------------------------------------------------------------
// Worker result relay
// ---------------------------------------------------------------------------

/// Message sent from the spawned worker task back to the event loop.
struct ResultMessage {
    job_id: JobId,
    segment_id: usize,
    node_id: NodeId,
    /// SRT port reserved for this segment's output (unused in K8s mode).
    srt_port: u16,
    /// `s3://bucket/key` of the uploaded output, if the worker used S3.
    output_uri: Option<String>,
    outcome: Result<SegmentResult>,
}

/// Outcome of a single worker invocation: the encoder result plus an
/// optional object-store URI where the encoded output was uploaded.
struct WorkerOutcome {
    result: SegmentResult,
    output_uri: Option<String>,
}

/// Result of assembling a job's per-segment outputs into a final file.
struct AssemblyResult {
    total_segments: usize,
    outcome: Result<String>,
}

/// Download every segment's encoded output, concatenate them via ffmpeg's
/// concat demuxer (stream copy — segments share the same codec/settings
/// since one job encodes all of them with the same config), and upload the
/// assembled file back to the object store.
///
/// Always produces an MP4 container regardless of the job's requested
/// `format` — HLS assembly (playlist + segment files) isn't implemented.
async fn assemble_output(
    store: &ObjectStore,
    job_id: JobId,
    _format: &str,
    mut uris: Vec<(usize, String)>,
) -> Result<String> {
    use tokio::process::Command;

    uris.sort_by_key(|(id, _)| *id);

    let work_dir = std::env::temp_dir().join(format!("transcoder-assemble-{}", job_id));
    tokio::fs::create_dir_all(&work_dir).await?;

    let mut list_lines = String::new();
    for (id, uri) in &uris {
        let seg_path = work_dir.join(format!("seg_{}.ts", id));
        ObjectStore::download_to_file(uri, store, &seg_path)
            .await
            .with_context(|| format!("failed to download segment output {}", uri))?;
        list_lines.push_str(&format!("file '{}'\n", seg_path.display()));
    }
    let list_path = work_dir.join("concat.txt");
    tokio::fs::write(&list_path, list_lines).await?;

    let output_path = work_dir.join("output.mp4");
    let status = Command::new("ffmpeg")
        .args(["-y", "-f", "concat", "-safe", "0", "-i"])
        .arg(&list_path)
        .args(["-c", "copy"])
        .arg(&output_path)
        .status()
        .await
        .context("failed to run ffmpeg concat")?;
    if !status.success() {
        anyhow::bail!("ffmpeg concat exited with {:?}", status.code());
    }

    let out_key = format!("jobs/{}/final/output.mp4", job_id);
    store
        .upload_file(&out_key, &output_path)
        .await
        .context("failed to upload assembled output")?;

    tokio::fs::remove_dir_all(&work_dir).await.ok();

    Ok(store.build_uri(&out_key))
}

// ---------------------------------------------------------------------------
// Video analysis (simplified)
// ---------------------------------------------------------------------------

/// Analyze input video and produce segment descriptors.
///
/// In a full implementation this invokes the coordinator's analyzer module.
/// For the initial version we produce a simple time-based segmentation.
fn analyze_video(data: &JobSubmitData) -> Vec<SegmentDescriptor> {
    // Estimate duration assuming ~5 MB/s average bitrate.
    let estimated_duration_secs = (data.input_size_bytes as f64) / (5.0 * 1024.0 * 1024.0);
    let segment_duration = 10.0_f64;
    let num_segments = ((estimated_duration_secs / segment_duration).ceil() as usize).max(1);

    let fps_estimate = 30.0_f64;
    let frames_per_segment = (segment_duration * fps_estimate) as u64;

    (0..num_segments)
        .map(|i| SegmentDescriptor {
            id: i,
            start_frame: i as u64 * frames_per_segment,
            end_frame: (i as u64 + 1) * frames_per_segment,
            start_timestamp: i as f64 * segment_duration,
            end_timestamp: ((i + 1) as f64 * segment_duration).min(estimated_duration_secs),
            lookahead_frames: Some(30),
            complexity_estimate: 0.5,
            scene_changes: vec![],
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Real segmentation (K8s mode, object-store-backed input)
// ---------------------------------------------------------------------------

const K8S_SEGMENT_DURATION_SECS: f64 = 10.0;

/// Download the source video from `source_uri`, cut it into fixed-duration
/// segments with ffmpeg's segment muxer (stream copy, keyframe-aligned —
/// no re-encoding here, the actual encode happens per-segment on workers),
/// upload each piece to the object store at the key the scheduler already
/// expects, and return real segment descriptors built from ffprobe's
/// reported duration.
async fn segment_and_upload(
    store: &ObjectStore,
    job_id: JobId,
    source_uri: &str,
) -> Result<Vec<SegmentDescriptor>> {
    use tokio::process::Command;

    let work_dir = std::env::temp_dir().join(format!("transcoder-segment-{}", job_id));
    tokio::fs::create_dir_all(&work_dir).await?;
    let source_path = work_dir.join("source.mp4");

    info!(%job_id, uri = %source_uri, "Fetching source video from object store");
    ObjectStore::download_to_file(source_uri, store, &source_path)
        .await
        .context("failed to download source video")?;

    // Real duration via ffprobe — the crude byte-size estimate this replaces
    // was wrong by roughly 6x on the video that exposed this whole gap.
    let probe = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(&source_path)
        .output()
        .await
        .context("failed to run ffprobe")?;
    if !probe.status.success() {
        anyhow::bail!(
            "ffprobe failed: {}",
            String::from_utf8_lossy(&probe.stderr).trim()
        );
    }
    let duration_secs: f64 = String::from_utf8_lossy(&probe.stdout)
        .trim()
        .parse()
        .context("failed to parse ffprobe duration")?;

    // Cut into fixed-duration chunks. `-f segment` snaps each boundary to
    // the nearest keyframe, so actual segment lengths vary slightly from
    // K8S_SEGMENT_DURATION_SECS — that's fine, we only need the total count
    // and approximate per-segment timestamps for scheduling/logging.
    let segment_pattern = work_dir.join("seg_%03d.ts");
    let cut = Command::new("ffmpeg")
        .args(["-y", "-i"])
        .arg(&source_path)
        .args([
            "-c",
            "copy",
            "-map",
            "0",
            "-f",
            "segment",
            "-segment_time",
            &K8S_SEGMENT_DURATION_SECS.to_string(),
            "-reset_timestamps",
            "1",
        ])
        .arg(&segment_pattern)
        .output()
        .await
        .context("failed to run ffmpeg segment cut")?;
    if !cut.status.success() {
        anyhow::bail!(
            "ffmpeg segment cut failed: {}",
            String::from_utf8_lossy(&cut.stderr).trim()
        );
    }

    let mut segment_files: Vec<PathBuf> = Vec::new();
    let mut read_dir = tokio::fs::read_dir(&work_dir)
        .await
        .context("failed to read segment work dir")?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("seg_") && n.ends_with(".ts"))
        {
            segment_files.push(path);
        }
    }
    segment_files.sort();
    if segment_files.is_empty() {
        anyhow::bail!("ffmpeg produced no segment files");
    }

    let fps_estimate = 30.0_f64;
    let mut descriptors = Vec::with_capacity(segment_files.len());
    for (i, path) in segment_files.iter().enumerate() {
        let key = format!("jobs/{}/input/seg_{}.ts", job_id, i);
        store
            .upload_file(&key, path)
            .await
            .with_context(|| format!("failed to upload segment {} to {}", i, key))?;

        let start = i as f64 * K8S_SEGMENT_DURATION_SECS;
        let end = ((i + 1) as f64 * K8S_SEGMENT_DURATION_SECS).min(duration_secs);
        let frames_in_segment = ((end - start) * fps_estimate).max(1.0) as u64;
        descriptors.push(SegmentDescriptor {
            id: i,
            start_frame: 0,
            end_frame: frames_in_segment,
            start_timestamp: start,
            end_timestamp: end,
            lookahead_frames: Some(30),
            complexity_estimate: 0.5,
            scene_changes: vec![],
        });
    }

    info!(
        %job_id,
        duration_secs,
        segment_count = descriptors.len(),
        "Segmented and uploaded source video"
    );

    // Best-effort cleanup — an emptyDir scratch volume, not worth failing
    // the job over if this doesn't succeed.
    let _ = tokio::fs::remove_dir_all(&work_dir).await;

    Ok(descriptors)
}

// ---------------------------------------------------------------------------
// Worker process spawning
// ---------------------------------------------------------------------------

/// Spawn the transcoder-worker binary for a single segment.
///
/// Returns a `WorkerOutcome` on success. Fetches input and uploads output
/// through the object store when `object_store` is `Some` and the transfer
/// URL is an `s3://` URI; otherwise uses the SRT data plane.
async fn spawn_worker(
    worker_binary: &PathBuf,
    lib_dir: &PathBuf,
    data: &SegmentAssignData,
    node_id: NodeId,
    _listen_addr: SocketAddr,
    _srt_port: u16,
    object_store: Option<&ObjectStore>,
) -> Result<WorkerOutcome> {
    use tokio::process::Command;

    let start_time = std::time::Instant::now();
    let tmp_dir = std::env::temp_dir().join(format!("transcoder-{}", data.job_id));
    tokio::fs::create_dir_all(&tmp_dir).await.ok();

    let input_path = tmp_dir.join(format!("segment_{}.ts", data.segment.id));
    let output_path = tmp_dir.join(format!("segment_{}_out.ts", data.segment.id));

    // Step 1: Fetch the segment data. The URI scheme determines the transport —
    // S3 (K8s mode) vs SRT (LAN / desktop).
    if !data.srt_url.is_empty() {
        if is_s3_uri(&data.srt_url) {
            let store = object_store.ok_or_else(|| {
                anyhow::anyhow!("received s3:// assignment but no object-store client configured")
            })?;
            info!(
                segment_id = data.segment.id,
                uri = %data.srt_url,
                "Fetching segment data from object store"
            );
            ObjectStore::download_to_file(&data.srt_url, store, &input_path)
                .await
                .context("Failed to fetch segment from object store")?;
        } else {
            info!(
                segment_id = data.segment.id,
                srt_url = %data.srt_url,
                "Fetching segment data via SRT"
            );
            SrtServer::fetch_file(&data.srt_url, &input_path)
                .await
                .context("Failed to fetch segment via SRT")?;
        }
    }

    // Step 2: Spawn the worker binary.
    let lib_path_key = if cfg!(target_os = "macos") {
        "DYLD_LIBRARY_PATH"
    } else {
        "LD_LIBRARY_PATH"
    };

    // transcoder-worker takes the segment descriptor as one JSON blob (see
    // worker/src/main.rs Args::segment), not discrete --segment-id/
    // --start-frame/--end-frame flags — those don't exist on its CLI at
    // all. --worker-id is required (no default). --presplit tells it to
    // skip seek/timestamp-filtering logic, which is correct here: the
    // input file it receives is always a single already-cut segment (via
    // segment_and_upload's ffmpeg -f segment step, or the equivalent on
    // the SRT/desktop path), never the full source video.
    let segment_json = serde_json::to_string(&data.segment)
        .context("failed to serialize segment descriptor")?;

    let mut cmd = Command::new(worker_binary);
    cmd.env(lib_path_key, lib_dir)
        .arg("--input")
        .arg(&input_path)
        .arg("--output")
        .arg(&output_path)
        .arg("--worker-id")
        .arg("0")
        .arg("--segment")
        .arg(&segment_json)
        .arg("--presplit")
        .arg("--crf")
        .arg(data.encoding_config.crf.to_string())
        .arg("--preset")
        .arg(&data.encoding_config.preset)
        .arg("--encoder")
        .arg(&data.encoding_config.encoder)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    info!(
        segment_id = data.segment.id,
        binary = %worker_binary.display(),
        "Spawning worker process"
    );

    let output = cmd
        .output()
        .await
        .context("Failed to spawn worker process")?;

    let elapsed = start_time.elapsed();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "Worker exited with {}: {}",
            output.status,
            stderr.trim()
        );
    }

    // Step 3: Parse the result. Try structured JSON from stdout first,
    // fall back to synthesized metadata.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let result = serde_json::from_str::<SegmentResult>(stdout.trim()).unwrap_or_else(|_| {
        let output_size = std::fs::metadata(&output_path)
            .map(|m| m.len())
            .unwrap_or(0);
        SegmentResult {
            segment_id: data.segment.id,
            worker_id: 0,
            node_id,
            frames_encoded: data.segment.end_frame.saturating_sub(data.segment.start_frame),
            output_size_bytes: output_size,
            encoding_time_secs: elapsed.as_secs_f64(),
            average_complexity: data.segment.complexity_estimate,
            scene_changes_detected: data.segment.scene_changes.len() as u64,
        }
    });

    // Step 4: Upload the encoded output to the object store if we're in
    // K8s mode (input URI was s3://). The master will see the resulting
    // URI in SegmentCompleteData.srt_output_url and fetch from there.
    let output_uri = if is_s3_uri(&data.srt_url) {
        let store = object_store.ok_or_else(|| {
            anyhow::anyhow!("s3:// input but no object-store client for output upload")
        })?;
        let out_key = format!("jobs/{}/output/seg_{}.ts", data.job_id, data.segment.id);
        info!(
            segment_id = data.segment.id,
            key = %out_key,
            "Uploading encoded output to object store"
        );
        store
            .upload_file(&out_key, &output_path)
            .await
            .context("Failed to upload encoded segment to object store")?;
        Some(store.build_uri(&out_key))
    } else {
        None
    };

    Ok(WorkerOutcome { result, output_uri })
}

// ---------------------------------------------------------------------------
// Banner
// ---------------------------------------------------------------------------

fn print_banner(node_id: NodeId, listen_addr: &SocketAddr, role: &str, caps: &NodeCapabilities) {
    eprintln!();
    eprintln!("  +================================================+");
    eprintln!("  |       TRANSCODER NODE  v{}               |", env!("CARGO_PKG_VERSION"));
    eprintln!("  +================================================+");
    eprintln!();
    eprintln!("  Node ID   : {}", node_id);
    eprintln!("  Listen    : {}", listen_addr);
    eprintln!("  Role      : {}", role);
    eprintln!("  Hostname  : {}", caps.hostname);
    eprintln!("  CPU cores : {}", caps.cpu_cores);
    eprintln!("  Memory    : {} MB", caps.available_memory_mb);
    eprintln!(
        "  GPU       : {}",
        caps.gpu_encoder.as_deref().unwrap_or("none")
    );
    eprintln!("  Workers   : {} max", caps.max_concurrent_workers);
    eprintln!();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize structured logging.
    let filter = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .init();

    // Generate a unique node identity.
    let node_id = Uuid::new_v4();
    let listen_addr: SocketAddr = cli
        .listen
        .parse()
        .context("Invalid --listen address")?;

    // Detect local machine capabilities.
    let mut capabilities = NodeManager::detect_capabilities();
    if let Some(ref name) = cli.name {
        capabilities.hostname = name.clone();
    }

    // Determine initial role for the banner.
    let initial_role = if cli.join.is_some() {
        "worker (joining cluster)"
    } else {
        "master (bootstrap)"
    };
    print_banner(node_id, &listen_addr, initial_role, &capabilities);

    // Create the WebSocket transport layer.
    let mut transport = Transport::new(listen_addr);
    let mut incoming_rx = transport
        .take_incoming()
        .expect("incoming receiver already taken");

    // Start accepting WebSocket connections.
    transport.listen().await?;
    info!("WebSocket transport listening on {}", listen_addr);

    // Optional object-storage client. Bucket comes from OBJECT_STORE_BUCKET
    // (set via ConfigMap in the overlays) with a sane default for dev.
    let object_store = if let Some(ref url) = cli.object_store_url {
        let bucket = std::env::var("OBJECT_STORE_BUCKET")
            .unwrap_or_else(|_| "transcoder-segments".to_string());
        info!(endpoint = %url, bucket = %bucket, "Connecting to object store");
        Some(
            ObjectStore::connect(url, bucket)
                .await
                .context("Failed to connect to object store")?,
        )
    } else {
        None
    };

    // Build application state.
    let mut app = App::new(
        node_id,
        listen_addr,
        capabilities,
        transport,
        cli.worker_binary,
        cli.lib_dir,
        cli.srt_base_port,
        object_store,
    );

    // Register ourselves in the node table.
    app.register_self();

    // --- Cluster bootstrap or join ---

    // In k8s-mode, pod ordinal decides the role. Ordinal 0 is always master;
    // every other pod joins and treats pod-0 as the leader without running a
    // bully election (K8s handles the restart/failover loop).
    let pod_ordinal: Option<usize> = if cli.k8s_mode {
        match std::env::var("POD_ORDINAL").ok().and_then(|v| v.parse().ok()) {
            Some(o) => Some(o),
            None => {
                warn!("--k8s-mode set but POD_ORDINAL env is missing; treating as ordinal 0");
                Some(0)
            }
        }
    } else {
        None
    };

    if let Some(ref join_addr) = cli.join {
        info!(addr = %join_addr, "Joining existing cluster");
        // Retry the join — under K8s, pod-N can boot before pod-0's DNS A
        // record is published, and the master pod itself may take a few
        // seconds to begin accepting WebSocket connections.
        let peer_handle = {
            let mut last_err: Option<anyhow::Error> = None;
            let mut handle = None;
            for attempt in 1..=30 {
                match app.transport.connect(join_addr).await {
                    Ok(h) => { handle = Some(h); break; }
                    Err(e) => {
                        warn!(addr = %join_addr, attempt, error = %e, "join attempt failed; retrying in 2s");
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
            handle.ok_or_else(|| {
                last_err.unwrap_or_else(|| anyhow::anyhow!("join exhausted retries"))
            }).with_context(|| format!("Failed to connect to {}", join_addr))?
        };

        // Use the resolved SocketAddr from the peer handle so DNS-based
        // join addresses (K8s service DNS) route correctly through send_to.
        let join_sock: SocketAddr = peer_handle.addr;
        let hello = HelloData {
            cluster_name: CLUSTER_NAME.into(),
            protocol_version: PROTOCOL_VERSION,
            master_id: None,
        };
        if let Ok(msg) = Message::new(OpCode::Hello, &hello) {
            let _ = app.transport.send_to(&join_sock, msg);
        }

        // k8s-mode: no bully election. Pod-0 is authoritative; others wait
        // for the master identity to arrive via the Identified handshake.
        if pod_ordinal.is_none() {
            let start_msg = app.election.start_election();
            app.transport.broadcast(&start_msg, None);
        } else {
            info!("k8s-mode: skipping bully election; trusting pod-0 as master");
        }
    } else if pod_ordinal == Some(0) {
        info!("k8s-mode: pod-0 bootstrapping as deterministic master");
        app.election.force_leader();
    } else if pod_ordinal.is_some() {
        // Shouldn't happen — non-zero ordinals are expected to have --join set
        // in the StatefulSet command.
        anyhow::bail!(
            "k8s-mode on non-zero pod ordinal requires --join <service>-0.<service>:9900"
        );
    } else {
        // Single-node bootstrap — we are the master.
        info!("Bootstrapping as single-node cluster master");
        app.election.force_leader();
        info!("Master ready, waiting for workers to join");
    }

    // ------------------------------------------------------------------
    // Main event loop
    // ------------------------------------------------------------------

    let mut heartbeat_tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut election_tick = tokio::time::interval(ELECTION_CHECK_INTERVAL);
    let mut health_tick = tokio::time::interval(HEALTH_CHECK_INTERVAL);
    let mut schedule_tick = tokio::time::interval(SCHEDULE_INTERVAL);

    // Consume the first immediate tick.
    heartbeat_tick.tick().await;
    election_tick.tick().await;
    health_tick.tick().await;
    schedule_tick.tick().await;

    info!("Entering main event loop");

    // K8s sends SIGTERM on pod stop; install a handler so the pre-stop hook
    // can trigger a clean NodeLeave. On non-unix, shim with a never-ready
    // future so the select! branch compiles identically.
    let sigterm_future = async {
        #[cfg(unix)]
        {
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            )
            .expect("Failed to install SIGTERM handler");
            sigterm.recv().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(sigterm_future);

    loop {
        tokio::select! {
            // Incoming WebSocket messages from peers.
            Some(peer_msg) = incoming_rx.recv() => {
                app.handle_message(peer_msg).await;
            }

            // Periodic heartbeat broadcast.
            _ = heartbeat_tick.tick() => {
                app.send_heartbeat();
            }

            // Election timeout check.
            _ = election_tick.tick() => {
                app.check_election();
            }

            // Health check: detect dead nodes.
            _ = health_tick.tick() => {
                app.check_health();
            }

            // Schedule pending segments to available workers (master only).
            _ = schedule_tick.tick() => {
                app.run_scheduler();
                app.poll_worker_results();
                app.poll_assembly_results();
            }

            // Graceful shutdown on Ctrl+C.
            _ = signal::ctrl_c() => {
                info!("Received SIGINT");
                eprintln!("\n  Shutting down gracefully...");
                app.send_leave();
                tokio::time::sleep(Duration::from_millis(250)).await;
                info!("Goodbye.");
                break;
            }

            // Graceful shutdown on SIGTERM (K8s pre-stop / pod eviction).
            _ = &mut sigterm_future => {
                info!("Received SIGTERM");
                app.send_leave();
                tokio::time::sleep(Duration::from_millis(250)).await;
                info!("Goodbye.");
                break;
            }
        }
    }

    Ok(())
}
