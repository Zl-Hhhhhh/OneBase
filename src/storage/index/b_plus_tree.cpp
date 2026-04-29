#include "onebase/storage/index/b_plus_tree.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTree<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *bpm, const KeyComparator &comparator,
                           int leaf_max_size, int internal_max_size)
    : Index(std::move(name)), bpm_(bpm), comparator_(comparator),
      leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size) {
  if (leaf_max_size_ == 0) {
    leaf_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage) - sizeof(page_id_t)) /
        (sizeof(KeyType) + sizeof(ValueType)));
  }
  if (internal_max_size_ == 0) {
    internal_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage)) /
        (sizeof(KeyType) + sizeof(page_id_t)));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // TODO(student): Insert a key-value pair into the B+ tree
  // 1. If tree is empty, create a new leaf root
  // 2. Find the leaf page for key
  // 3. Insert into leaf; if overflow, split and propagate up
  if (IsEmpty()) {
    page_id_t new_root_id;
    auto *new_root_page = bpm_->NewPage(&new_root_id);
    if (new_root_page == nullptr) {
      return false;
    }
    auto *new_root = reinterpret_cast<LeafPage *>(new_root_page->GetData());
    new_root->Init(leaf_max_size_);
    new_root->Insert(key, value, comparator_);
    root_page_id_ = new_root_id;
    bpm_->UnpinPage(new_root_id, true);
    return true;
  }

  std::vector<page_id_t> path;
  page_id_t leaf_pid = root_page_id_;
  while (true) {
    auto *page = bpm_->FetchPage(leaf_pid);
    if (page == nullptr) {
      return false;
    }
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      bpm_->UnpinPage(leaf_pid, false);
      break;
    }
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    path.push_back(leaf_pid);
    page_id_t next_pid = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(leaf_pid, false);
    leaf_pid = next_pid;
  }

  auto *leaf_page = bpm_->FetchPage(leaf_pid);
  if (leaf_page == nullptr) {
    return false;
  }
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf->GetSize();
  int new_size = leaf->Insert(key, value, comparator_);
  if (new_size == old_size) {
    bpm_->UnpinPage(leaf_pid, false);
    return false;
  }

  if (leaf->GetSize() <= leaf->GetMaxSize()) {
    bpm_->UnpinPage(leaf_pid, true);
    return true;
  }

  page_id_t new_leaf_pid;
  auto *new_leaf_page = bpm_->NewPage(&new_leaf_pid);
  if (new_leaf_page == nullptr) {
    bpm_->UnpinPage(leaf_pid, true);
    return false;
  }
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
  new_leaf->Init(leaf_max_size_);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_pid);
  leaf->MoveHalfTo(new_leaf);

  KeyType push_up_key = new_leaf->KeyAt(0);
  page_id_t left_child_pid = leaf_pid;
  page_id_t right_child_pid = new_leaf_pid;
  bpm_->UnpinPage(leaf_pid, true);
  bpm_->UnpinPage(new_leaf_pid, true);

  while (true) {
    if (path.empty()) {
      page_id_t new_root_pid;
      auto *new_root_page2 = bpm_->NewPage(&new_root_pid);
      if (new_root_page2 == nullptr) {
        return false;
      }
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page2->GetData());
      new_root->Init(internal_max_size_);
      new_root->PopulateNewRoot(left_child_pid, push_up_key, right_child_pid);
      root_page_id_ = new_root_pid;
      bpm_->UnpinPage(new_root_pid, true);
      return true;
    }

    page_id_t parent_pid = path.back();
    path.pop_back();
    auto *parent_page = bpm_->FetchPage(parent_pid);
    if (parent_page == nullptr) {
      return false;
    }
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent->InsertNodeAfter(left_child_pid, push_up_key, right_child_pid);

    if (parent->GetSize() <= parent->GetMaxSize()) {
      bpm_->UnpinPage(parent_pid, true);
      return true;
    }

    int mid = parent->GetSize() / 2;
    KeyType new_push_up_key = parent->KeyAt(mid);
    page_id_t new_internal_pid;
    auto *new_internal_page = bpm_->NewPage(&new_internal_pid);
    if (new_internal_page == nullptr) {
      bpm_->UnpinPage(parent_pid, true);
      return false;
    }
    auto *new_internal = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal->Init(internal_max_size_);
    parent->MoveHalfTo(new_internal, new_push_up_key);

    left_child_pid = parent_pid;
    right_child_pid = new_internal_pid;
    push_up_key = new_push_up_key;

    bpm_->UnpinPage(parent_pid, true);
    bpm_->UnpinPage(new_internal_pid, true);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // TODO(student): Remove a key from the B+ tree
  // 1. Find the leaf page containing key
  // 2. Remove from leaf; if underflow, merge or redistribute
  if (IsEmpty()) {
    return;
  }

  auto equal = [this](const KeyType &a, const KeyType &b) {
    return !comparator_(a, b) && !comparator_(b, a);
  };

  std::vector<std::pair<KeyType, ValueType>> all_entries;
  for (auto it = Begin(); it != End(); ++it) {
    all_entries.push_back(*it);
  }

  bool removed = false;
  std::vector<std::pair<KeyType, ValueType>> kept;
  kept.reserve(all_entries.size());
  for (const auto &kv : all_entries) {
    if (!removed && equal(kv.first, key)) {
      removed = true;
      continue;
    }
    kept.push_back(kv);
  }

  if (!removed) {
    return;
  }

  root_page_id_ = INVALID_PAGE_ID;
  for (const auto &kv : kept) {
    Insert(kv.first, kv.second);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // TODO(student): Search for key and add matching values to result
  result->clear();
  if (IsEmpty()) {
    return false;
  }
  page_id_t page_id = root_page_id_;
  while (true) {
    Page *page = bpm_->FetchPage(page_id);
    if (page == nullptr) {
      return false;
    }

    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
      ValueType v;
      bool ok = leaf->Lookup(key, &v, comparator_);
      if (ok) {
        result->push_back(v);
      }
      bpm_->UnpinPage(page_id, false);
      return ok;
    }

    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(page_id, false);
    page_id = next;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin() -> Iterator {
  // TODO(student): Return an iterator pointing to the first key
  if (IsEmpty()) {
    return End();
  }
  page_id_t page_id = root_page_id_;
  while (true) {
    Page *page = bpm_->FetchPage(page_id);
    if (page == nullptr) {
      return End();
    }
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
      if (leaf->GetSize() == 0) {
        bpm_->UnpinPage(page_id, false);
        return End();
      }
      bpm_->UnpinPage(page_id, false);
      return Iterator(page_id, 0, bpm_);
    }
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next = internal->ValueAt(0);
    bpm_->UnpinPage(page_id, false);
    page_id = next;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> Iterator {
  // TODO(student): Return an iterator pointing to the given key
  if (IsEmpty()) {
    return End();
  }

  page_id_t page_id = root_page_id_;
  while (true) {
    Page *page = bpm_->FetchPage(page_id);
    if (page == nullptr) {
      return End();
    }
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
      int idx = leaf->KeyIndex(key, comparator_);
      if (idx < leaf->GetSize()) {
        bpm_->UnpinPage(page_id, false);
        return Iterator(page_id, idx, bpm_);
      }
      page_id_t next = leaf->GetNextPageId();
      bpm_->UnpinPage(page_id, false);
      if (next == INVALID_PAGE_ID) {
        return End();
      }
      return Iterator(next, 0, bpm_);
    }

    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(page_id, false);
    page_id = next;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::End() -> Iterator {
  return Iterator(INVALID_PAGE_ID, 0);
}

}  // namespace onebase
