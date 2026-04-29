#include "onebase/storage/page/b_plus_tree_leaf_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeLeafPage<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  // TODO(student): Binary search for the index of key
  int left = 0, right = GetSize();
  while (left < right) {
    int mid=left+(right-left)/2;
    if(comparator(array_[mid].first,key)){
      left=mid+1;
    }else{
      right=mid;
    }
  }
  return left;
  //throw NotImplementedException("BPlusTreeLeafPage::KeyIndex");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                         const KeyComparator &comparator) const -> bool {
  // TODO(student): Look up a key and return its associated value
  int idx = KeyIndex(key, comparator);
  if (idx >= GetSize() || comparator(array_[idx].first, key) || comparator(key, array_[idx].first)) {
    return false;
  }
  *value = array_[idx].second;
  return true;
  //throw NotImplementedException("BPlusTreeLeafPage::Lookup");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                         const KeyComparator &comparator) -> int {
  // TODO(student): Insert a key-value pair in sorted order
  int idx = KeyIndex(key, comparator);
  if (idx < GetSize() && !comparator(array_[idx].first, key) && !comparator(key, array_[idx].first)) {
    return GetSize();  // key already exists
  }
  for (int i = GetSize(); i > idx; --i) {
    array_[i] = array_[i - 1];
  }
  array_[idx] = {key, value};
  IncreaseSize(1);
  return GetSize();
  //throw NotImplementedException("BPlusTreeLeafPage::Insert");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                        const KeyComparator &comparator) -> int {
  // TODO(student): Remove a key-value pair
  int idx = KeyIndex(key, comparator);
  if (idx >= GetSize() || comparator(array_[idx].first, key) || comparator(key, array_[idx].first)) {
    return GetSize();
  }
  for (int i = idx; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
  //throw NotImplementedException("BPlusTreeLeafPage::RemoveAndDeleteRecord");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move second half of entries to recipient during split
  int move_start = GetSize() / 2;
  int move_count = GetSize() - move_start;

  int recipient_start = recipient->GetSize();
  for (int i = 0; i < move_count; ++i) {
    recipient->array_[recipient_start + i] = array_[move_start + i];
  }
  IncreaseSize(-move_count);
  recipient->IncreaseSize(move_count);
  //throw NotImplementedException("BPlusTreeLeafPage::MoveHalfTo");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move all entries to recipient during merge
  int recipient_start = recipient->GetSize();
  for (int i = 0; i < GetSize(); ++i) {
    recipient->array_[recipient_start + i] = array_[i];
  }
  recipient->IncreaseSize(GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
  //throw NotImplementedException("BPlusTreeLeafPage::MoveAllTo");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move first entry to end of recipient
  if (GetSize() == 0) {
    return;
  }

  int recipient_size = recipient->GetSize();
  recipient->array_[recipient_size] = array_[0];
  recipient->IncreaseSize(1);
  for (int i = 1; i < GetSize(); ++i) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  //throw NotImplementedException("BPlusTreeLeafPage::MoveFirstToEndOf");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // TODO(student): Move last entry to front of recipient
  if (GetSize() == 0) {
    return;
  }
  auto moved = array_[GetSize() - 1];
  for (int i = recipient->GetSize(); i > 0; --i) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = moved;
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
  //throw NotImplementedException("BPlusTreeLeafPage::MoveLastToFrontOf");
}

}  // namespace onebase
