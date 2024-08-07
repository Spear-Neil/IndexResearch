#include "N.h"

#include <assert.h>

#include <algorithm>

#include "N16.cpp"
#include "N256.cpp"
#include "N4.cpp"
#include "N48.cpp"

namespace ART_OLC_OptiQL {

void N::setType(NTypes type) { prefixCount.setType(convertTypeToPrefixCount(type)); }

uint32_t N::convertTypeToPrefixCount(NTypes type) { return (static_cast<uint32_t>(type) << 30); }

NTypes N::getType() const { return static_cast<NTypes>(prefixCount.getRaw() >> 30); }

N *N::getAnyChild(const N *node) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->getAnyChild();
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->getAnyChild();
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->getAnyChild();
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->getAnyChild();
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N::change(N *node, uint8_t key, N *val) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->change(key, val);
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      return n->change(key, val);
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      return n->change(key, val);
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      return n->change(key, val);
    }
  }
  assert(false);
  __builtin_unreachable();
}

template <typename curN, typename biggerN>
void N::insertGrow(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion, uint8_t keyParent,
                   uint8_t key, N *val, bool &needRestart, N *&obsoleteN) {
  if (!n->isFull()) {
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    DEFINE_CONTEXT(q, 0);
    UPGRADE_NODE(n);
    if (needRestart) return;
    n->insert(key, val);
    UNLOCK_NODE(n);
    return;
  }

  DEFINE_CONTEXT(parentQ, 1);
  DEFINE_CONTEXT(q, 0);
  UPGRADE_PARENT();
  if (needRestart) return;

  UPGRADE_NODE(n);
  if (needRestart) {
    UNLOCK_PARENT();
    return;
  }

  auto nBig = new biggerN(n->getPrefix(), n->getPrefixLength());
  n->copyTo(nBig);
  nBig->insert(key, val);

  N::change(parentNode, keyParent, nBig);

  n->setObsolete();
  UNLOCK_NODE(n);
  obsoleteN = n;
  UNLOCK_PARENT();
}

void N::insertAndUnlock(N *node, uint64_t v, N *parentNode, uint64_t parentVersion,
                        uint8_t keyParent, uint8_t key, N *val, bool &needRestart, N *&obsoleteN) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      insertGrow<N4, N16>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart,
                          obsoleteN);
      break;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      insertGrow<N16, N48>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart,
                           obsoleteN);
      break;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      insertGrow<N48, N256>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart,
                            obsoleteN);
      break;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      insertGrow<N256, N256>(n, v, parentNode, parentVersion, keyParent, key, val, needRestart,
                             obsoleteN);
      break;
    }
  }
}

inline N *N::getChild(const uint8_t k, const N *node) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->getChild(k);
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->getChild(k);
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->getChild(k);
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->getChild(k);
    }
  }
  assert(false);
  __builtin_unreachable();
}

void N::deleteChildren(N *node) {
  if (N::isLeaf(node)) {
    return;
  }
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      n->deleteChildren();
      return;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      n->deleteChildren();
      return;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      n->deleteChildren();
      return;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      n->deleteChildren();
      return;
    }
  }
  assert(false);
  __builtin_unreachable();
}

template <typename curN, typename smallerN>
void N::removeAndShrink(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion,
                        uint8_t keyParent, uint8_t key, bool &needRestart, N *&obsoleteN) {
  if (!n->isUnderfull() || parentNode == nullptr) {
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) return;
    }
    DEFINE_CONTEXT(q, 0);
    UPGRADE_NODE(n);
    if (needRestart) return;

    n->remove(key);
    UNLOCK_NODE(n);
    return;
  }
  DEFINE_CONTEXT(parentQ, 1);
  DEFINE_CONTEXT(q, 0);
  UPGRADE_PARENT();
  if (needRestart) return;

  UPGRADE_NODE(n);
  if (needRestart) {
    UNLOCK_PARENT();
    return;
  }

  auto nSmall = new smallerN(n->getPrefix(), n->getPrefixLength());

  n->copyTo(nSmall);
  nSmall->remove(key);
  N::change(parentNode, keyParent, nSmall);

  n->setObsolete();
  UNLOCK_NODE(n);
  obsoleteN = n;
  UNLOCK_PARENT();
}

void N::removeAndUnlock(N *node, uint64_t v, uint8_t key, N *parentNode, uint64_t parentVersion,
                        uint8_t keyParent, bool &needRestart, N *&obsoleteN) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      removeAndShrink<N4, N4>(n, v, parentNode, parentVersion, keyParent, key, needRestart,
                              obsoleteN);
      break;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      removeAndShrink<N16, N4>(n, v, parentNode, parentVersion, keyParent, key, needRestart,
                               obsoleteN);
      break;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      removeAndShrink<N48, N16>(n, v, parentNode, parentVersion, keyParent, key, needRestart,
                                obsoleteN);
      break;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      removeAndShrink<N256, N48>(n, v, parentNode, parentVersion, keyParent, key, needRestart,
                                 obsoleteN);
      break;
    }
  }
}

uint32_t N::getPrefixLength() const { return prefixCount.get(); }

bool N::hasPrefix() const { return prefixCount.get() > 0; }

uint32_t N::getCount() const { return count; }

const uint8_t *N::getPrefix() const { return prefix; }

void N::setPrefix(const uint8_t *prefix, uint32_t length) {
  if (length > 0) {
    memcpy(this->prefix, prefix, std::min(length, maxStoredPrefixLength));
    prefixCount = length;
  } else {
    prefixCount = 0;
  }
}

void N::addPrefixBefore(N *node, uint8_t key) {
  uint32_t prefixCopyCount = std::min(maxStoredPrefixLength, node->getPrefixLength() + 1);
  memmove(this->prefix + prefixCopyCount, this->prefix,
          std::min(this->getPrefixLength(), maxStoredPrefixLength - prefixCopyCount));
  memcpy(this->prefix, node->prefix, std::min(prefixCopyCount, node->getPrefixLength()));
  if (node->getPrefixLength() < maxStoredPrefixLength) {
    this->prefix[prefixCopyCount - 1] = key;
  }
  this->prefixCount += node->getPrefixLength() + 1;
}

bool N::isLeaf(const N *n) {
  return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << 63)) ==
         (static_cast<uint64_t>(1) << 63);
}

N *N::setLeaf(TID tid) { return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << 63)); }

TID N::getLeaf(const N *n) {
  return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << 63) - 1));
}

std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->getSecondChild(key);
    }
    default: {
      assert(false);
      __builtin_unreachable();
    }
  }
}

void N::deleteNode(N *node) {
  if (N::isLeaf(node)) {
    return;
  }
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      delete n;
      return;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      delete n;
      return;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      delete n;
      return;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      delete n;
      return;
    }
  }
  delete node;
}

TID N::getAnyChildTid(const N *n, bool &needRestart) {
  const N *nextNode = n;

  while (true) {
    const N *node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) return 0;

    nextNode = getAnyChild(node);
    node->readUnlockOrRestart(v, needRestart);
    if (needRestart) return 0;

    assert(nextNode != nullptr);
    if (isLeaf(nextNode)) {
      return getLeaf(nextNode);
    }
  }
}

uint64_t N::getChildren(const N *node, uint8_t start, uint8_t end,
                        std::tuple<uint8_t, N *> children[], uint32_t &childrenCount) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->getChildren(start, end, children, childrenCount);
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->getChildren(start, end, children, childrenCount);
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->getChildren(start, end, children, childrenCount);
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->getChildren(start, end, children, childrenCount);
    }
  }
  assert(false);
  __builtin_unreachable();
}
}  // namespace ART_OLC