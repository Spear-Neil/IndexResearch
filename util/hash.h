#ifndef UTIL_HASH_H
#define UTIL_HASH_H

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <tuple>
#include <cstring>
#include <byteswap.h>

namespace util {

namespace util_hash {

/**================ C++ standard hash ================*/
inline size_t standard_hash(void* key, size_t len) {
  const size_t seed = 0xc70f6907UL;
  return std::_Hash_bytes(key, len, seed);
}

/* directly return key as hash value */
inline size_t standard_hash(uint64_t key) {
  return std::hash<uint64_t>()(key);
}

inline size_t standard_hash(uint32_t key) {
  return std::hash<uint32_t>()(key);
}

/**================== jenkins hash ==================*/
inline size_t jenkins_hash(void* key, size_t len) {
  size_t i = 0, hash = 0;
  const char* data = (const char*) key;
  while(i != len) {
    hash += data[i++];
    hash += hash << 10;
    hash ^= hash >> 6;
  }
  hash += hash << 3;
  hash ^= hash >> 11;
  hash += hash >> 15;
  return hash;
}

inline size_t jenkins_hash(uint64_t key) {
  return jenkins_hash(&key, 8);
}

inline size_t jenkins_hash(uint32_t key) {
  return jenkins_hash(&key, 4);
}

/**================== murmur hash ==================*/
inline size_t murmur_hash32(void* key, size_t len) {
  const unsigned int seed = 0xc70f6907UL;
  const unsigned int m = 0x5bd1e995;
  const int r = 24;
  unsigned int h = seed ^ len;
  const unsigned char* data = (const unsigned char*) key;

  while(len >= 4) {
    unsigned int k = *(unsigned int*) data;
    k *= m;
    k ^= k >> r;
    k *= m;
    h *= m;
    h ^= k;
    data += 4;
    len -= 4;
  }

  switch(len) {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  }

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;
  return h;
}

inline size_t murmur_hash32(uint64_t key) {
  return murmur_hash32(&key, 8);
}

inline size_t murmur_hash32(uint32_t key) {
  return murmur_hash32(&key, 4);
}

inline size_t murmur_hash64(void* key, size_t len) {
  const uint64_t m = 0xc6a4a7935bd1e995ull;
  const std::size_t r = 47;
  uint64_t seed = 7079;

  uint64_t h = seed ^ (len * m);

  const auto* data = (const uint64_t*) key;
  const uint64_t* end = data + (len / 8);

  while(data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const auto* data2 = (const unsigned char*) data;

  switch(len & 7ull) {
    case 7:
      h ^= uint64_t(data2[6]) << 48ull;
    case 6:
      h ^= uint64_t(data2[5]) << 40ull;
    case 5:
      h ^= uint64_t(data2[4]) << 32ull;
    case 4:
      h ^= uint64_t(data2[3]) << 24ull;
    case 3:
      h ^= uint64_t(data2[2]) << 16ull;
    case 2:
      h ^= uint64_t(data2[1]) << 8ull;
    case 1:
      h ^= uint64_t(data2[0]);
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

inline size_t murmur_hash64(uint64_t key) {
  return murmur_hash64(&key, 8);
}

inline size_t murmur_hash64(uint32_t key) {
  return murmur_hash64(&key, 4);
}

/**================== faster hash ==================*/
inline uint64_t Rotr64(uint64_t x, size_t n) {
  return (((x) >> n) | ((x) << (64 - n)));
}

inline size_t faster_hash(void* key, size_t len) {
  // 40343 is a "magic constant" that works well,
  // 38299 is another good value.
  // Both are primes and have a good distribution of bits.
  const uint64_t kMagicNum = 40343;
  uint64_t hashState = len;
  const char* data = (const char*) key;

  for(size_t idx = 0; idx < len; ++idx) {
    hashState = kMagicNum * hashState + data[idx];
  }

  // The final scrambling helps with short keys that vary only on the high order bits.
  // Low order bits are not always well distributed so shift them to the high end, where they'll
  // form part of the 14-bit tag.
  return Rotr64(kMagicNum * hashState, 6);
}

inline size_t faster_hash(uint64_t key) {
  uint64_t local_rand = key;
  uint64_t local_rand_hash = 8;
  local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
  local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
  local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
  local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
  local_rand_hash = 40343 * local_rand_hash;
  return Rotr64(local_rand_hash, 43);
}

inline size_t faster_hash(uint32_t key) {
  return faster_hash(uint64_t(key));
}

/**=================== city hash ===================*/
typedef uint8_t uint8;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef std::pair<uint64, uint64> uint128;

// Some primes between 2^63 and 2^64 for various uses.
constexpr uint64 k0 = 0xc3a5c85c97cb3127ULL;
constexpr uint64 k1 = 0xb492b66fbe98f273ULL;
constexpr uint64 k2 = 0x9ae16a3b2f90404fULL;

inline uint64 Uint128Low64(const uint128& x) { return x.first; }

inline uint64 Uint128High64(const uint128& x) { return x.second; }

// Hash 128 input bits down to 64 bits of output.
// This is intended to be a reasonably good hash function.
inline uint64 Hash128to64(const uint128& x) {
  // Murmur-inspired hashing.
  const uint64 kMul = 0x9ddfea08eb382d69ULL;
  uint64 a = (Uint128Low64(x) ^ Uint128High64(x)) * kMul;
  a ^= (a >> 47);
  uint64 b = (Uint128High64(x) ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

inline uint64 Fetch64(const char* p) {
  uint64 result;
  memcpy(&result, p, sizeof(result));
  return result;
}

inline uint32 Fetch32(const char* p) {
  uint32 result;
  memcpy(&result, p, sizeof(result));
  return result;
}

// Bitwise right rotate.  Normally this will compile to a single
// instruction, especially if the shift is a manifest constant.
inline uint64 Rotate(uint64 val, int shift) {
  // Avoid shifting by 64: doing so yields an undefined result.
  return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

inline uint64 ShiftMix(uint64 val) {
  return val ^ (val >> 47);
}

inline uint64 HashLen16(uint64 u, uint64 v) {
  return Hash128to64(uint128(u, v));
}

inline uint64 HashLen16(uint64 u, uint64 v, uint64 mul) {
  // Murmur-inspired hashing.
  uint64 a = (u ^ v) * mul;
  a ^= (a >> 47);
  uint64 b = (v ^ a) * mul;
  b ^= (b >> 47);
  b *= mul;
  return b;
}

inline uint64 HashLen0to16(const char* s, size_t len) {
  if(len >= 8) {
    uint64 mul = k2 + len * 2;
    uint64 a = Fetch64(s) + k2;
    uint64 b = Fetch64(s + len - 8);
    uint64 c = Rotate(b, 37) * mul + a;
    uint64 d = (Rotate(a, 25) + b) * mul;
    return HashLen16(c, d, mul);
  }
  if(len >= 4) {
    uint64 mul = k2 + len * 2;
    uint64 a = Fetch32(s);
    return HashLen16(len + (a << 3), Fetch32(s + len - 4), mul);
  }
  if(len > 0) {
    uint8 a = static_cast<uint8>(s[0]);
    uint8 b = static_cast<uint8>(s[len >> 1]);
    uint8 c = static_cast<uint8>(s[len - 1]);
    uint32 y = static_cast<uint32>(a) + (static_cast<uint32>(b) << 8);
    uint32 z = static_cast<uint32>(len) + (static_cast<uint32>(c) << 2);
    return ShiftMix(y * k2 ^ z * k0) * k2;
  }
  return k2;
}

// This probably works well for 16-byte strings as well, but it may be overkill
// in that case.
inline uint64 HashLen17to32(const char* s, size_t len) {
  uint64 mul = k2 + len * 2;
  uint64 a = Fetch64(s) * k1;
  uint64 b = Fetch64(s + 8);
  uint64 c = Fetch64(s + len - 8) * mul;
  uint64 d = Fetch64(s + len - 16) * k2;
  return HashLen16(Rotate(a + b, 43) + Rotate(c, 30) + d,
                   a + Rotate(b + k2, 18) + c, mul);
}

// Return an 8-byte hash for 33 to 64 bytes.
inline uint64 HashLen33to64(const char* s, size_t len) {
  uint64 mul = k2 + len * 2;
  uint64 a = Fetch64(s) * k2;
  uint64 b = Fetch64(s + 8);
  uint64 c = Fetch64(s + len - 24);
  uint64 d = Fetch64(s + len - 32);
  uint64 e = Fetch64(s + 16) * k2;
  uint64 f = Fetch64(s + 24) * 9;
  uint64 g = Fetch64(s + len - 8);
  uint64 h = Fetch64(s + len - 16) * mul;
  uint64 u = Rotate(a + g, 43) + (Rotate(b, 30) + c) * 9;
  uint64 v = ((a + g) ^ d) + f + 1;
  uint64 w = bswap_64((u + v) * mul) + h;
  uint64 x = Rotate(e + f, 42) + c;
  uint64 y = (bswap_64((v + w) * mul) + g) * mul;
  uint64 z = e + f + c;
  a = bswap_64((x + z) * mul + y) + b;
  b = ShiftMix((z + a) * mul + d + h) * mul;
  return b + x;
}

// Return a 16-byte hash for 48 bytes.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
inline std::pair<uint64, uint64> WeakHashLen32WithSeeds(
  uint64 w, uint64 x, uint64 y, uint64 z, uint64 a, uint64 b) {
  a += w;
  b = Rotate(b + a + z, 21);
  uint64 c = a;
  a += x;
  a += y;
  b += Rotate(a, 44);
  return std::make_pair(a + z, b + c);
}

// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
inline std::pair<uint64, uint64> WeakHashLen32WithSeeds(
  const char* s, uint64 a, uint64 b) {
  return WeakHashLen32WithSeeds(Fetch64(s),
                                Fetch64(s + 8),
                                Fetch64(s + 16),
                                Fetch64(s + 24),
                                a,
                                b);
}

inline size_t city_hash(void* key, size_t len) {
  const char* s = (const char*) key;
  if(len <= 32) {
    if(len <= 16) {
      return HashLen0to16(s, len);
    } else {
      return HashLen17to32(s, len);
    }
  } else if(len <= 64) {
    return HashLen33to64(s, len);
  }

  // For strings over 64 bytes we hash the end first, and then as we
  // loop we keep 56 bytes of state: v, w, x, y, and z.
  uint64 x = Fetch64(s + len - 40);
  uint64 y = Fetch64(s + len - 16) + Fetch64(s + len - 56);
  uint64 z = HashLen16(Fetch64(s + len - 48) + len, Fetch64(s + len - 24));
  std::pair<uint64, uint64> v = WeakHashLen32WithSeeds(s + len - 64, len, z);
  std::pair<uint64, uint64> w = WeakHashLen32WithSeeds(s + len - 32, y + k1, x);
  x = x * k1 + Fetch64(s);

  // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
  len = (len - 1) & ~static_cast<size_t>(63);
  do {
    x = Rotate(x + y + v.first + Fetch64(s + 8), 37) * k1;
    y = Rotate(y + v.second + Fetch64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + Fetch64(s + 40);
    z = Rotate(z + w.first, 33) * k1;
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y + Fetch64(s + 16));
    std::swap(z, x);
    s += 64;
    len -= 64;
  } while(len != 0);
  return HashLen16(HashLen16(v.first, w.first) + ShiftMix(y) * k1 + z,
                   HashLen16(v.second, w.second) + x);
}

inline size_t city_hash(uint64 key) {
  return city_hash(&key, 8);
}

inline size_t city_hash(uint32_t key) {
  return city_hash(&key, 4);
}

}

inline size_t hash(void* key, size_t len) {
  return util_hash::city_hash(key, len);
}

inline size_t hash(uint64_t key) {
  return util_hash::faster_hash(key);
}

inline size_t hash(uint32_t key) {
  return util_hash::faster_hash(key);
}

inline size_t hash(int64_t key) {
  return hash(uint64_t(key));
}

inline size_t hash(int32_t key) {
  return hash(uint32_t(key));
}

}

#endif //UTIL_HASH_H
