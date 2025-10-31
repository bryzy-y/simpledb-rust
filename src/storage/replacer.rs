use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

struct LRUNode {
    frame_id: usize,
    access_times: VecDeque<Instant>,
    evictable: bool,
}

// Implementation of LRU-K replacer
pub struct LRUKReplacer {
    nodes: HashMap<usize, LRUNode>,
    k: usize,
    curr_size: u32,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        let nodes = HashMap::with_capacity(num_frames);
        Self {
            nodes,
            k,
            curr_size: 0,
        }
    }

    pub fn evict(&mut self) -> Option<usize> {
        let current = Instant::now();

        struct EvictCandidate {
            frame_id: Option<usize>,
            max_k_distance: std::time::Duration,
        }

        let mut candidate = EvictCandidate {
            frame_id: None,
            max_k_distance: Duration::ZERO,
        };
        let mut k_distance: Duration;

        for node in self.nodes.values().filter(|&n| n.evictable) {
            if node.access_times.len() < self.k {
                // Assign infinite k-distance if less than k access times
                k_distance = Duration::MAX;
            } else {
                // Make sure we have k access times
                debug_assert!(node.access_times.len() == self.k);
                k_distance = current.duration_since(node.access_times.front().unwrap().to_owned());
            }

            if k_distance > candidate.max_k_distance {
                candidate.frame_id = Some(node.frame_id);
                candidate.max_k_distance = k_distance;
            } else if k_distance == Duration::MAX && candidate.max_k_distance == Duration::MAX {
                // If both have MAX k-distance, use the one with the earliest overall access time

                // frame_id must be set if max_k_distance is MAX
                debug_assert!(candidate.frame_id.is_some());
                let current_candidate_node = self.nodes.get(&candidate.frame_id.unwrap()).unwrap();

                // Make sure both have less than k access times
                debug_assert!(current_candidate_node.access_times.len() < self.k);
                debug_assert!(node.access_times.len() < self.k);

                // Compare the least-recent recorded access times for both nodes
                let min = Instant::min(
                    current_candidate_node
                        .access_times
                        .front()
                        .unwrap()
                        .to_owned(),
                    node.access_times.front().unwrap().to_owned(),
                );
                if min == *node.access_times.front().unwrap() {
                    candidate.frame_id = Some(node.frame_id);
                }
            }
        }
        // No evictable frame found
        candidate.frame_id
    }

    pub fn record_access(&mut self, frame_id: usize) {
        let timestamp = Instant::now();
        match self.nodes.get_mut(&frame_id) {
            Some(node) => {
                if node.access_times.len() == self.k {
                    node.access_times.pop_front();
                    node.access_times.push_back(timestamp);
                } else {
                    node.access_times.push_back(timestamp);
                }
            }
            None => {
                // New node
                let mut history = VecDeque::with_capacity(self.k);
                history.push_back(timestamp);

                let node = LRUNode {
                    frame_id,
                    access_times: history,
                    evictable: false,
                };
                self.nodes.insert(frame_id, node);
            }
        }
    }

    pub fn remove(&mut self, frame_id: usize) {
        if let Some(node) = self.nodes.remove(&frame_id) {
            if node.evictable {
                self.curr_size -= 1;
            }
        }
    }

    pub fn set_evictable(&mut self, frame_id: usize, evictable: bool) {
        if let Some(node) = self.nodes.get_mut(&frame_id) {
            if node.evictable != evictable {
                node.evictable = evictable;
                if evictable {
                    self.curr_size += 1;
                } else {
                    self.curr_size -= 1;
                }
            }
        }
    }

    pub fn size(&self) -> usize {
        self.curr_size as usize
    }
}
