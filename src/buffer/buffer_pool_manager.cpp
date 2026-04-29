#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/common/logger.h"

namespace onebase {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // TODO(student): Allocate a new page in the buffer pool
  // 1. Pick a victim frame from free list or replacer
  // 2. If victim is dirty, write it back to disk
  // 3. Allocate a new page_id via disk_manager_
  // 4. Update page_table_ and page metadata
  std::scoped_lock guard(latch_);

  frame_id_t victim_frame_id;
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr; 
  }
  else if(!free_list_.empty()){//用freelist中的frame
    victim_frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else{//用replacer中的frame
    if (!replacer_->Evict(&victim_frame_id)) {
      return nullptr;
    }
  }
  Page &victim_page = pages_[victim_frame_id];

  page_id_t old_page_id = victim_page.GetPageId();
  if (old_page_id != INVALID_PAGE_ID) {
    if (victim_page.IsDirty()) {
      disk_manager_->WritePage(old_page_id, victim_page.GetData());
    }
    page_table_.erase(old_page_id);
  }

  page_id_t new_page_id = disk_manager_->AllocatePage();
  *page_id = new_page_id;
  victim_page.ResetMemory();
  victim_page.page_id_ = new_page_id;
  victim_page.is_dirty_ = false;
  victim_page.pin_count_ = 1;
  page_table_[new_page_id] = victim_frame_id;
  replacer_->RecordAccess(victim_frame_id);
  replacer_->SetEvictable(victim_frame_id, false);
  return &victim_page;
  //throw NotImplementedException("BufferPoolManager::NewPage");
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  // TODO(student): Fetch a page from the buffer pool
  // 1. Search page_table_ for existing mapping
  // 2. If not found, pick a victim frame
  // 3. Read page from disk into the frame
  std::scoped_lock guard(latch_);
  auto it = page_table_.find(page_id);
  if(it!=page_table_.end()){
    frame_id_t frame_id=it->second;
    Page &page=pages_[frame_id];
    page.pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id,false);
    return &page;
  }
  else{
    frame_id_t victim_frame_id;
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr; 
    }
    else if(!free_list_.empty()){
      victim_frame_id = free_list_.front();
      free_list_.pop_front();
    }
    else{
      if (!replacer_->Evict(&victim_frame_id)) {
        return nullptr;
      }
    }
    Page &victim_page = pages_[victim_frame_id];

    page_id_t old_page_id = victim_page.GetPageId();
    if (old_page_id != INVALID_PAGE_ID) {
      if (victim_page.IsDirty()) {
        disk_manager_->WritePage(old_page_id, victim_page.GetData());
      }
      page_table_.erase(old_page_id);
    }

    disk_manager_->ReadPage(page_id, victim_page.GetData());
    victim_page.page_id_ = page_id;
    victim_page.is_dirty_ = false;
    victim_page.pin_count_ = 1;
    page_table_[page_id] = victim_frame_id;
    replacer_->RecordAccess(victim_frame_id);
    replacer_->SetEvictable(victim_frame_id, false);
    return &victim_page;
  }
  //throw NotImplementedException("BufferPoolManager::FetchPage");
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  // TODO(student): Unpin a page, decrementing pin count
  // - If pin_count reaches 0, set evictable in replacer
  std::scoped_lock guard(latch_);
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()){
    return false;
  }
  frame_id_t frame_id=it->second;
  Page &page=pages_[frame_id];
  if(page.GetPinCount()<=0){
    return false;
  }
  page.pin_count_--;
  if(is_dirty){
    page.is_dirty_=true;
  }
  if(page.GetPinCount()==0){
    replacer_->SetEvictable(frame_id,true);
  }
  return true;
  //throw NotImplementedException("BufferPoolManager::UnpinPage");
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // TODO(student): Delete a page from the buffer pool
  // - Page must have pin_count == 0
  // - Remove from page_table_, reset memory, add frame to free_list_
  std::scoped_lock guard(latch_);
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()){
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  frame_id_t frame_id=it->second;
  Page &page=pages_[frame_id];
  if(page.GetPinCount()>0){
    return false;
  }
  if(page.IsDirty()){
    disk_manager_->WritePage(page_id,page.GetData());
  }
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  page.ResetMemory();
  page.page_id_=INVALID_PAGE_ID;
  page.is_dirty_=false;
  page.pin_count_=0;
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  return true;
  //throw NotImplementedException("BufferPoolManager::DeletePage");
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // TODO(student): Force flush a page to disk regardless of dirty flag
  std::scoped_lock guard(latch_);
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()){
    return false;
  }
  frame_id_t frame_id=it->second;
  Page &page=pages_[frame_id];
  disk_manager_->WritePage(page_id,page.GetData());
  page.is_dirty_=false;
  return true;
  //throw NotImplementedException("BufferPoolManager::FlushPage");
}

void BufferPoolManager::FlushAllPages() {
  // TODO(student): Flush all pages in the buffer pool to disk
  std::scoped_lock guard(latch_);
  for(auto &entry:page_table_){
    frame_id_t frame_id=entry.second;
    Page &page=pages_[frame_id];
    if(page.IsDirty()){
      disk_manager_->WritePage(entry.first,page.GetData());
      page.is_dirty_=false;
    }
  }
  //throw NotImplementedException("BufferPoolManager::FlushAllPages");
}

}  // namespace onebase
