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

        candidate
            .frame_id
            .inspect(|frame_id| self.remove(*frame_id))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lruk_replacer() {
        let mut replacer = LRUKReplacer::new(7, 2);

        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(3);
        replacer.record_access(4);
        replacer.record_access(5);
        replacer.record_access(6);

        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        replacer.set_evictable(5, true);
        replacer.set_evictable(6, false);

        assert_eq!(replacer.size(), 5);

        // Frame 1 now has 2 accesses
        replacer.record_access(1);

        // All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
        // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.evict(), Some(3));
        assert_eq!(replacer.evict(), Some(4));
        assert_eq!(replacer.size(), 2);
        // Now the replacer has the frames [5, 1].

        // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
        replacer.record_access(3);
        replacer.record_access(4);
        replacer.record_access(5);
        replacer.record_access(4);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);

        assert_eq!(replacer.size(), 4);
        assert_eq!(replacer.evict(), Some(3));
        assert_eq!(replacer.size(), 3);

        // Set 6 to be evictable. 6 should be evicted next since it has the maximum backward k-distance.
        replacer.set_evictable(6, true);
        assert_eq!(replacer.size(), 4);
        assert_eq!(replacer.evict(), Some(6));
        assert_eq!(replacer.size(), 3);

        // Mark frame 1 as non-evictable. We now have [5, 4].
        replacer.set_evictable(1, false);
        assert_eq!(replacer.size(), 2);
        assert_eq!(replacer.evict(), Some(5));
        assert_eq!(replacer.size(), 1);

        replacer.record_access(1);
        replacer.record_access(1);
        replacer.set_evictable(1, true);

        assert_eq!(replacer.evict(), Some(4));
        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.size(), 0);

        // Insert frame 1 again and mark it as non-evictable.
        replacer.record_access(1);
        replacer.set_evictable(1, false);
        assert_eq!(replacer.size(), 0);

        assert_eq!(replacer.evict(), None);
        replacer.set_evictable(1, true);

        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.size(), 0);

        // There is nothing left in the replacer, so make sure this doesn't do something strange.
        let frame_id = replacer.evict();
        assert_eq!(frame_id, None);
        assert_eq!(replacer.size(), 0);

        replacer.set_evictable(6, true);
        assert_eq!(replacer.size(), 0);
        replacer.set_evictable(6, false);
    }
}
