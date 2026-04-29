#include "onebase/storage/page/b_plus_tree_internal_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeInternalPage<int, page_id_t, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  // TODO(student): Find the index of the given value in the internal page
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value) {
      return i;
    }
  }
  return -1;
  //throw NotImplementedException("BPlusTreeInternalPage::ValueIndex");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // TODO(student): Find the child page that should contain the given key
  ValueType result = ValueAt(0);

  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(key, KeyAt(i))) {
      break;  // key < Ki，应该走上一段对应的孩子
    }
    result = ValueAt(i);  // key >= Ki，更新到当前段
  }
  return result;
  //throw NotImplementedException("BPlusTreeInternalPage::Lookup");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) {
  // TODO(student): Create a new root with one key and two children
  array_[0].second = old_value;
  array_[1].first = key;
  array_[1].second = new_value;
  SetSize(2);
  //throw NotImplementedException("BPlusTreeInternalPage::PopulateNewRoot");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) -> int {
  // TODO(student): Insert a new key-value pair after old_value
  int old_idx = ValueIndex(old_value);
  if (old_idx == -1) {
    return -1;
  }
  //在old_idx+1位置插入
  for (int i = GetSize(); i > old_idx + 1; --i) {
    array_[i] = array_[i - 1];
  }
  array_[old_idx + 1].first = key;
  array_[old_idx + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
  //throw NotImplementedException("BPlusTreeInternalPage::InsertNodeAfter");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // TODO(student): Remove the key-value pair at the given index
  if(index>=GetSize()){
    return;
  }
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  //throw NotImplementedException("BPlusTreeInternalPage::Remove");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  // TODO(student): Remove all entries and return the only remaining child
  ValueType result = ValueAt(0);
  SetSize(0);
  return result;
  //throw NotImplementedException("BPlusTreeInternalPage::RemoveAndReturnOnlyChild");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move all entries to recipient during merge
  int sizeof_rec=recipient->GetSize();
  recipient->array_[sizeof_rec].first = middle_key;
  recipient->array_[sizeof_rec].second = array_[0].second;
  for(int i=1;i<GetSize();++i){
    recipient->array_[sizeof_rec+i]=array_[i];
  }
  recipient->SetSize(sizeof_rec+GetSize());
  SetSize(0);
  //throw NotImplementedException("BPlusTreeInternalPage::MoveAllTo");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move the second half of entries to recipient during split
  int n=GetSize();
  int mid=n/2;
  recipient->array_[0].first = middle_key;
  recipient->array_[0].second = array_[mid].second;
  for(int i=mid+1;i<n;++i){
    recipient->array_[i-mid]=array_[i];
  }
  recipient->SetSize(GetSize()-mid);
  SetSize(mid);
  //throw NotImplementedException("BPlusTreeInternalPage::MoveHalfTo");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move first entry to end of recipient (redistribute)
  //向右借元素
  recipient->array_[recipient->GetSize()].first = middle_key;
  recipient->array_[recipient->GetSize()].second = array_[0].second;
  recipient->IncreaseSize(1);
  for(int i=0;i<GetSize()-1;i++){
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
  //throw NotImplementedException("BPlusTreeInternalPage::MoveFirstToEndOf");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // TODO(student): Move last entry to front of recipient (redistribute)
  //向左借元素
  for(int i=recipient->GetSize();i>0;i--){
    recipient->array_[i]=recipient->array_[i-1];
  }
  recipient->array_[0].first = middle_key;
  recipient->array_[0].second = array_[GetSize()-1].second;
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
  //throw NotImplementedException("BPlusTreeInternalPage::MoveLastToFrontOf");
}

}  // namespace onebase
