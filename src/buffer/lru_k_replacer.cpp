#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_frames_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // TODO(student): Implement LRU-K eviction policy
  // - Find the frame with the largest backward k-distance
  // - Among frames with fewer than k accesses, evict the one with earliest first access
  // - Only consider evictable frames
  std::scoped_lock guard(latch_);

  frame_id_t inf_victim = INVALID_FRAME_ID;
  size_t inf_oldest_ts = 0;
  bool has_inf = false;

  frame_id_t finite_victim = INVALID_FRAME_ID;
  size_t max_k_distance = 0;
  bool has_finite = false;

  for (const auto &[fid, entry] : entries_) {
    if (!entry.is_evictable_) {
      continue;
    }
    if (entry.history_.empty()) {
      continue;
    }

    if (entry.history_.size() < k_) {
      size_t first_ts = entry.history_.front();
      if (!has_inf || first_ts < inf_oldest_ts) {
        has_inf = true;
        inf_oldest_ts = first_ts;
        inf_victim = fid;
      }
    } else {
      size_t k_distance = current_timestamp_ - entry.history_.front();
      if (!has_finite || k_distance > max_k_distance) {
        has_finite = true;
        max_k_distance = k_distance;
        finite_victim = fid;
      }
    }
  }

  frame_id_t victim = INVALID_FRAME_ID;
  if (has_inf) {
    victim = inf_victim;
  } else if (has_finite) {
    victim = finite_victim;
  } else {
    return false;
  }

  entries_.erase(victim);
  curr_size_--;
  *frame_id = victim;
  return true;
  //throw NotImplementedException("LRUKReplacer::Evict");
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // TODO(student): Record a new access for frame_id at current timestamp
  // - If frame_id is new, create an entry
  // - Add current_timestamp_ to the frame's history
  // - Increment current_timestamp_
  std::scoped_lock guard(latch_);
  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    entries_[frame_id] = FrameEntry();
    it = entries_.find(frame_id);
  }
  it->second.history_.push_back(current_timestamp_++);
  if (it->second.history_.size() > k_) {
    it->second.history_.pop_front();
  }
  // throw NotImplementedException("LRUKReplacer::RecordAccess");
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO(student): Set whether a frame is evictable
  // - Update curr_size_ accordingly
  std::scoped_lock guard(latch_);
  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return; 
  }
  if(set_evictable && !it->second.is_evictable_) {
    curr_size_++;
  } else if (!set_evictable && it->second.is_evictable_) {
    curr_size_--;
  }
  it->second.is_evictable_ = set_evictable;
  // throw NotImplementedException("LRUKReplacer::SetEvictable");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // TODO(student): Remove a frame from the replacer
  // - The frame must be evictable; throw if not
  std::scoped_lock guard(latch_);
  auto it = entries_.find(frame_id);
  if(it==entries_.end())
    return;
  if(!it->second.is_evictable_) {
    throw std::invalid_argument("Frame is not evictable");
  }
  if(it->second.is_evictable_) {
    curr_size_--;
    entries_.erase(it);
  }
  //throw NotImplementedException("LRUKReplacer::Remove");
}

auto LRUKReplacer::Size() const -> size_t {
  // TODO(student): Return the number of evictable frames
  return curr_size_;
  // throw NotImplementedException("LRUKReplacer::Size");
}

}  // namespace onebase