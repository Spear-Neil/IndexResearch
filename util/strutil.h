#ifndef UTIL_STRUTIL_H
#define UTIL_STRUTIL_H

#include <cstddef>
#include <cstring>
#include <algorithm>
#include <string>
#include <sstream>
#include <vector>

namespace util {

/**
 * compare string s1 with s2, if s1 < s2, the return value is less than zero
 * if s1 == s2, return is zero, if s1 > s2, the return value is greater than zero
 */
inline int compare(char* s1, size_t n1, char* s2, size_t n2) {
  size_t len = std::min(n1, n2);
  int r = std::memcmp(s1, s2, len);
  if(r == 0) {
    if(n1 < n2) r = -1;
    else if(n1 > n2) r = 1;
  }
  return r;
}

/**
 * extracting the length of the common prefix between s1 and s2
 */
inline size_t common_prefix(char* s1, size_t n1, char* s2, size_t n2) {
  size_t idx, psize = std::min(n1, n2);
  for(idx = 0; idx < psize; idx++)
    if(s1[idx] != s2[idx]) break;
  return idx;
}

/**
 * split string str to substrings, delim - delimiter
 */
inline std::vector<std::string> string_split(std::string&& str, char delim) {
  std::vector<std::string> ret;
  std::istringstream input(str);
  std::string sub;

  while(std::getline(input, sub, delim))
    ret.push_back(std::move(sub));

  return ret;
}

inline std::vector<std::string> string_split(const std::string& str, char delim) {
  std::vector<std::string> ret;
  std::istringstream input(str);
  std::string sub;

  while(std::getline(input, sub, delim))
    ret.push_back(std::move(sub));

  return ret;
}

}

#endif //UTIL_STRUTIL_H
