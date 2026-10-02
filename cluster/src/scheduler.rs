//! Segment scheduling and work distribution.
//!
//! Distributes video segments across cluster nodes based on their capabilities
//! and current load. Handles reassignment when nodes fail.

use std::collections::{HashMap, VecDeque};

use tracing::{debug, info, warn};

use crate::protocol::{JobId, NodeId, NodeInfo, NodeStatus, SegmentDescriptor, SegmentResult};

/// A segment assignment record.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub job_id: JobId,
    pub segment: SegmentDescriptor,
    pub node_id: NodeId,
}

/// Per-job scheduling state.
#[derive(Debug)]
struct JobSchedule {
    total_segments: usize,
    pending: VecDeque<SegmentDescriptor>,
    assigned: HashMap<usize, NodeId>,       // segment_id -> node
    completed: HashMap<usize, SegmentResult>,
    failed: HashMap<usize, String>,
}

/// Distributes segments across cluster nodes based on capability and load.
pub struct Scheduler {
    jobs: HashMap<JobId, JobSchedule>,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("jobs", &self.jobs.len())
            .field(
                "total_pending",
                &self.jobs.values().map(|j| j.pending.len()).sum::<usize>(),
            )
            .field(
                "total_assigned",
                &self.jobs.values().map(|j| j.assigned.len()).sum::<usize>(),
            )
            .finish()
    }
}

impl Scheduler {
    /// Create a new empty scheduler.
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    /// Add segments for a new job to the scheduling queue.
    /// Segments are sorted by complexity (highest first) for better load balancing.
    pub fn add_job(&mut self, job_id: JobId, mut segments: Vec<SegmentDescriptor>) {
        // Sort by complexity descending -- assign hardest segments first
        segments.sort_by(|a, b| {
            b.complexity_estimate
                .partial_cmp(&a.complexity_estimate)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total = segments.len();
        let pending: VecDeque<_> = segments.into_iter().collect();

        info!(%job_id, segment_count = total, "Scheduler: added job");
        self.jobs.insert(
            job_id,
            JobSchedule {
                total_segments: total,
                pending,
                assigned: HashMap::new(),
                completed: HashMap::new(),
                failed: HashMap::new(),
            },
        );
    }

    /// Schedule pending segments to available nodes.
    ///
    /// Algorithm:
    /// 1. Filter to Active nodes with available capacity
    /// 2. Sort by available slots * cpu_cores (favor powerful nodes with capacity)
    /// 3. Assign most complex segments to most powerful nodes first
    /// 4. Return list of assignments
    pub fn schedule(&mut self, nodes: &[NodeInfo]) -> Vec<Assignment> {
        let mut assignments = Vec::new();

        // Build list of nodes with available capacity
        let mut available: Vec<(&NodeInfo, usize)> = nodes
            .iter()
            .filter(|n| matches!(n.status, NodeStatus::Active))
            .map(|n| {
                let slots = n
                    .capabilities
                    .max_concurrent_workers
                    .saturating_sub(n.load.active_workers);
                (n, slots)
            })
            .filter(|(_, slots)| *slots > 0)
            .collect();

        if available.is_empty() {
            debug!("Scheduler: no available nodes");
            return assignments;
        }

        // Sort by (available slots * cpu_cores) descending -- favor powerful nodes with capacity
        available.sort_by(|a, b| {
            let score_a = a.1 * a.0.capabilities.cpu_cores;
            let score_b = b.1 * b.0.capabilities.cpu_cores;
            score_b.cmp(&score_a)
        });

        // Track remaining slots per node
        let mut node_slots: HashMap<NodeId, usize> =
            available.iter().map(|(n, s)| (n.node_id, *s)).collect();

        // Ordered list of node IDs by power (most powerful first)
        let node_order: Vec<NodeId> = available.iter().map(|(n, _)| n.node_id).collect();

        // Iterate over all jobs and assign pending segments
        for (job_id, schedule) in self.jobs.iter_mut() {
            if schedule.pending.is_empty() {
                continue;
            }

            let mut unassigned = VecDeque::new();

            while let Some(segment) = schedule.pending.pop_front() {
                // Find the best node with remaining slots
                let best_node = node_order
                    .iter()
                    .find(|nid| node_slots.get(nid).copied().unwrap_or(0) > 0);

                if let Some(&node_id) = best_node {
                    let seg_id = segment.id;
                    debug!(
                        %job_id,
                        segment_id = seg_id,
                        %node_id,
                        complexity = segment.complexity_estimate,
                        "Assigning segment"
                    );

                    schedule.assigned.insert(seg_id, node_id);
                    assignments.push(Assignment {
                        job_id: *job_id,
                        segment,
                        node_id,
                    });

                    if let Some(slots) = node_slots.get_mut(&node_id) {
                        *slots = slots.saturating_sub(1);
                    }
                } else {
                    // No more available slots
                    unassigned.push_back(segment);
                }
            }

            // Return unassigned segments to the pending queue
            schedule.pending = unassigned;
        }

        if !assignments.is_empty() {
            info!(
                assigned = assignments.len(),
                "Scheduler: scheduling complete"
            );
        }

        assignments
    }

    /// Mark a segment as completed.
    pub fn mark_complete(&mut self, job_id: &JobId, result: SegmentResult) {
        if let Some(schedule) = self.jobs.get_mut(job_id) {
            let seg_id = result.segment_id;
            info!(
                %job_id,
                segment_id = seg_id,
                node_id = %result.node_id,
                time_secs = result.encoding_time_secs,
                "Segment completed"
            );
            schedule.assigned.remove(&seg_id);
            schedule.completed.insert(seg_id, result);
        }
    }

    /// Mark a segment as failed.
    pub fn mark_failed(&mut self, job_id: &JobId, segment_id: usize, error: String) {
        if let Some(schedule) = self.jobs.get_mut(job_id) {
            warn!(%job_id, segment_id, %error, "Segment failed");
            schedule.assigned.remove(&segment_id);
            schedule.failed.insert(segment_id, error);
        }
    }

    /// Reassign all segments from a dead/failed node back to the pending queue.
    /// Returns the number of segments requeued.
    ///
    /// Note: Since we only track segment IDs (not full descriptors) in the assigned map,
    /// the caller should use `requeue_segment` with the original descriptors for precise
    /// re-scheduling. This method creates minimal placeholder descriptors.
    pub fn reassign_from_node(&mut self, dead_node_id: &NodeId) -> usize {
        let mut requeued = 0;

        for (job_id, schedule) in self.jobs.iter_mut() {
            let affected: Vec<usize> = schedule
                .assigned
                .iter()
                .filter(|(_, node)| *node == dead_node_id)
                .map(|(seg_id, _)| *seg_id)
                .collect();

            for seg_id in affected {
                schedule.assigned.remove(&seg_id);
                // Create a minimal segment descriptor for re-queuing.
                // In practice, the master should keep the original descriptors
                // and use requeue_segment() instead.
                let segment = SegmentDescriptor {
                    id: seg_id,
                    start_frame: 0,
                    end_frame: 0,
                    start_timestamp: 0.0,
                    end_timestamp: 0.0,
                    lookahead_frames: None,
                    complexity_estimate: 0.5,
                    scene_changes: vec![],
                    copy: false,
                };
                warn!(
                    %job_id,
                    segment_id = seg_id,
                    %dead_node_id,
                    "Requeuing segment from dead node"
                );
                schedule.pending.push_back(segment);
                requeued += 1;
            }
        }

        if requeued > 0 {
            info!(
                %dead_node_id,
                requeued,
                "Segments requeued from failed node"
            );
        }

        requeued
    }

    /// Requeue a specific segment with its full descriptor (e.g., after node failure).
    pub fn requeue_segment(&mut self, job_id: &JobId, segment: SegmentDescriptor) {
        if let Some(schedule) = self.jobs.get_mut(job_id) {
            let seg_id = segment.id;
            schedule.assigned.remove(&seg_id);
            schedule.failed.remove(&seg_id);
            schedule.pending.push_back(segment);
            info!(%job_id, segment_id = seg_id, "Requeued segment");
        }
    }

    /// Get progress for a job: (completed, total, failed).
    pub fn job_progress(&self, job_id: &JobId) -> Option<(usize, usize, usize)> {
        self.jobs.get(job_id).map(|s| {
            (s.completed.len(), s.total_segments, s.failed.len())
        })
    }

    /// Check if a job is fully complete (all segments done or failed).
    pub fn is_job_complete(&self, job_id: &JobId) -> bool {
        self.jobs
            .get(job_id)
            .map(|s| s.completed.len() + s.failed.len() >= s.total_segments)
            .unwrap_or(false)
    }

    /// Check if a job succeeded (all segments completed, none failed).
    pub fn is_job_successful(&self, job_id: &JobId) -> bool {
        self.jobs
            .get(job_id)
            .map(|s| s.completed.len() >= s.total_segments && s.failed.is_empty())
            .unwrap_or(false)
    }

    /// Check if a job has any failed segments.
    pub fn has_failures(&self, job_id: &JobId) -> bool {
        self.jobs
            .get(job_id)
            .map(|s| !s.failed.is_empty())
            .unwrap_or(false)
    }

    /// Get completed segment results for a job, sorted by segment ID.
    pub fn get_results(&self, job_id: &JobId) -> Vec<SegmentResult> {
        self.jobs
            .get(job_id)
            .map(|s| {
                let mut results: Vec<_> = s.completed.values().cloned().collect();
                results.sort_by_key(|r| r.segment_id);
                results
            })
            .unwrap_or_default()
    }

    /// Get pending segment count for a job.
    pub fn pending_count(&self, job_id: &JobId) -> usize {
        self.jobs.get(job_id).map(|s| s.pending.len()).unwrap_or(0)
    }

    /// Get assigned (in-flight) segment count for a job.
    pub fn assigned_count(&self, job_id: &JobId) -> usize {
        self.jobs
            .get(job_id)
            .map(|s| s.assigned.len())
            .unwrap_or(0)
    }

    /// Get the nodes that a job's segments are currently assigned to.
    pub fn job_assigned_nodes(&self, job_id: &JobId) -> Vec<NodeId> {
        self.jobs
            .get(job_id)
            .map(|s| {
                let mut nodes: Vec<NodeId> = s.assigned.values().copied().collect();
                nodes.sort();
                nodes.dedup();
                nodes
            })
            .unwrap_or_default()
    }

    /// Remove a completed or failed job from the scheduler.
    pub fn remove_job(&mut self, job_id: &JobId) {
        self.jobs.remove(job_id);
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{NodeCapabilities, NodeLoad};
    use uuid::Uuid;

    fn make_segment(id: usize, complexity: f32) -> SegmentDescriptor {
        SegmentDescriptor {
            id,
            start_frame: (id * 1000) as u64,
            end_frame: ((id + 1) * 1000) as u64,
            start_timestamp: id as f64 * 10.0,
            end_timestamp: (id + 1) as f64 * 10.0,
            lookahead_frames: Some(30),
            complexity_estimate: complexity,
            scene_changes: vec![],
            copy: false,
        }
    }

    fn make_node(id: NodeId, cores: usize, max_workers: usize, active: usize) -> NodeInfo {
        NodeInfo {
            node_id: id,
            address: "127.0.0.1:9000".into(),
            capabilities: NodeCapabilities {
                cpu_cores: cores,
                available_memory_mb: 16384,
                has_gpu: false,
                gpu_encoder: None,
                hostname: "test".into(),
                os: "test".into(),
                max_concurrent_workers: max_workers,
            },
            load: NodeLoad {
                active_workers: active,
                cpu_usage_percent: 0.0,
                memory_used_mb: 0,
                segments_completed: 0,
                segments_failed: 0,
            },
            is_master: false,
            last_heartbeat_ms: 0,
            status: NodeStatus::Active,
        }
    }

    fn make_result(segment_id: usize, node_id: NodeId) -> SegmentResult {
        SegmentResult {
            segment_id,
            worker_id: 0,
            node_id,
            frames_encoded: 1000,
            output_size_bytes: 500000,
            encoding_time_secs: 5.0,
            average_complexity: 0.5,
            scene_changes_detected: 1,
        }
    }

    #[test]
    fn test_add_job() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        scheduler.add_job(
            job_id,
            vec![make_segment(0, 0.5), make_segment(1, 0.8)],
        );
        assert_eq!(scheduler.pending_count(&job_id), 2);
    }

    #[test]
    fn test_schedule_distributes_to_available_nodes() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();

        scheduler.add_job(
            job_id,
            vec![
                make_segment(0, 0.3),
                make_segment(1, 0.7),
                make_segment(2, 0.5),
            ],
        );

        let nodes = vec![
            make_node(node_a, 8, 2, 0), // 2 slots
            make_node(node_b, 4, 2, 1), // 1 slot
        ];

        let assignments = scheduler.schedule(&nodes);
        assert_eq!(assignments.len(), 3);
        assert_eq!(scheduler.pending_count(&job_id), 0);
    }

    #[test]
    fn test_schedule_respects_capacity() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_a = Uuid::new_v4();

        scheduler.add_job(
            job_id,
            vec![
                make_segment(0, 0.5),
                make_segment(1, 0.5),
                make_segment(2, 0.5),
            ],
        );

        // Only 1 slot available
        let nodes = vec![make_node(node_a, 4, 2, 1)];

        let assignments = scheduler.schedule(&nodes);
        assert_eq!(assignments.len(), 1);
        assert_eq!(scheduler.pending_count(&job_id), 2);
    }

    #[test]
    fn test_schedule_no_available_nodes() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);

        let node_a = Uuid::new_v4();
        let nodes = vec![make_node(node_a, 4, 2, 2)]; // fully busy

        let assignments = scheduler.schedule(&nodes);
        assert!(assignments.is_empty());
        assert_eq!(scheduler.pending_count(&job_id), 1);
    }

    #[test]
    fn test_schedule_empty() {
        let mut scheduler = Scheduler::new();
        let nodes = vec![make_node(Uuid::new_v4(), 4, 2, 0)];
        let assignments = scheduler.schedule(&nodes);
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_complex_segments_to_powerful_nodes() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let powerful = Uuid::new_v4();
        let weak = Uuid::new_v4();

        scheduler.add_job(
            job_id,
            vec![
                make_segment(0, 0.9), // very complex
                make_segment(1, 0.1), // simple
            ],
        );

        let nodes = vec![
            make_node(powerful, 16, 1, 0), // 16 cores, 1 slot
            make_node(weak, 2, 1, 0),      // 2 cores, 1 slot
        ];

        let assignments = scheduler.schedule(&nodes);
        assert_eq!(assignments.len(), 2);

        // The most complex segment (0.9, sorted first) should go to the powerful node
        let first_assignment = &assignments[0];
        assert_eq!(first_assignment.segment.complexity_estimate, 0.9);
        assert_eq!(first_assignment.node_id, powerful);
    }

    #[test]
    fn test_mark_complete() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);
        let nodes = vec![make_node(node_id, 4, 2, 0)];
        scheduler.schedule(&nodes);

        scheduler.mark_complete(&job_id, make_result(0, node_id));

        let (completed, total, failed) = scheduler.job_progress(&job_id).unwrap();
        assert_eq!(completed, 1);
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert!(scheduler.is_job_complete(&job_id));
        assert!(scheduler.is_job_successful(&job_id));
    }

    #[test]
    fn test_mark_failed() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);
        let nodes = vec![make_node(node_id, 4, 2, 0)];
        scheduler.schedule(&nodes);

        scheduler.mark_failed(&job_id, 0, "encoder crashed".into());

        assert!(scheduler.has_failures(&job_id));
        assert!(scheduler.is_job_complete(&job_id));
        assert!(!scheduler.is_job_successful(&job_id));
    }

    #[test]
    fn test_reassign_from_node() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();

        scheduler.add_job(
            job_id,
            vec![make_segment(0, 0.5), make_segment(1, 0.5)],
        );

        // Assign both to node_a
        let nodes = vec![make_node(node_a, 4, 4, 0)];
        let assignments = scheduler.schedule(&nodes);
        assert_eq!(assignments.len(), 2);

        // node_a dies -- reassign its segments
        let requeued = scheduler.reassign_from_node(&node_a);
        assert_eq!(requeued, 2);
        assert_eq!(scheduler.pending_count(&job_id), 2);
        assert_eq!(scheduler.assigned_count(&job_id), 0);

        // Re-schedule to node_b
        let nodes = vec![make_node(node_b, 4, 4, 0)];
        let assignments = scheduler.schedule(&nodes);
        assert_eq!(assignments.len(), 2);
        assert!(assignments.iter().all(|a| a.node_id == node_b));
    }

    #[test]
    fn test_requeue_segment() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);
        let nodes = vec![make_node(node_id, 4, 2, 0)];
        scheduler.schedule(&nodes);
        assert_eq!(scheduler.assigned_count(&job_id), 1);

        scheduler.requeue_segment(&job_id, make_segment(0, 0.5));
        assert_eq!(scheduler.assigned_count(&job_id), 0);
        assert_eq!(scheduler.pending_count(&job_id), 1);
    }

    #[test]
    fn test_get_results_sorted() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        scheduler.add_job(
            job_id,
            vec![
                make_segment(0, 0.5),
                make_segment(1, 0.5),
                make_segment(2, 0.5),
            ],
        );

        // Complete out of order
        scheduler.mark_complete(&job_id, make_result(2, node_id));
        scheduler.mark_complete(&job_id, make_result(0, node_id));
        scheduler.mark_complete(&job_id, make_result(1, node_id));

        let results = scheduler.get_results(&job_id);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].segment_id, 0);
        assert_eq!(results[1].segment_id, 1);
        assert_eq!(results[2].segment_id, 2);
    }

    #[test]
    fn test_job_progress_unknown_job() {
        let scheduler = Scheduler::new();
        assert!(scheduler.job_progress(&Uuid::new_v4()).is_none());
    }

    #[test]
    fn test_remove_job() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);
        assert!(scheduler.job_progress(&job_id).is_some());

        scheduler.remove_job(&job_id);
        assert!(scheduler.job_progress(&job_id).is_none());
    }

    #[test]
    fn test_job_assigned_nodes() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();

        scheduler.add_job(
            job_id,
            vec![
                make_segment(0, 0.5),
                make_segment(1, 0.5),
                make_segment(2, 0.5),
            ],
        );

        let nodes = vec![
            make_node(node_a, 8, 2, 0),
            make_node(node_b, 4, 2, 0),
        ];

        scheduler.schedule(&nodes);
        let assigned_nodes = scheduler.job_assigned_nodes(&job_id);
        assert!(!assigned_nodes.is_empty());
        assert!(assigned_nodes.len() <= 2);
    }

    #[test]
    fn test_busy_nodes_excluded() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);

        // Node is Busy status
        let mut node = make_node(node_id, 4, 2, 0);
        node.status = NodeStatus::Busy;
        let nodes = vec![node];

        let assignments = scheduler.schedule(&nodes);
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_dead_nodes_excluded() {
        let mut scheduler = Scheduler::new();
        let job_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        scheduler.add_job(job_id, vec![make_segment(0, 0.5)]);

        let mut node = make_node(node_id, 4, 2, 0);
        node.status = NodeStatus::Dead;
        let nodes = vec![node];

        let assignments = scheduler.schedule(&nodes);
        assert!(assignments.is_empty());
    }
}
