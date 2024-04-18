#include <iostream>
#include <cstring>
#include "strutil.h"

using namespace util;

int main(int argc, char* argv[]) {
  std::cout << "-- strutil example" << std::endl;

  char* s1 = "abcdfalkfjaldkfjaldkf";
  char* s2 = "abcdefajldfkjalkfjaldkfj";

  std::cout << "s1 ? s2: ";
  int ret = compare(s1, strlen(s1), s2, strlen(s2));
  if(ret == 0) std::cout << "=";
  else if(ret < 0) std::cout << "<";
  else std::cout << ">";
  std::cout << std::endl;

  std::cout << "common prefix: " << std::string(s1, common_prefix(s1, strlen(s1), s2, strlen(s2))) << std::endl;

  std::cout << "sub string delimited by 'd': " << std::endl;
  for(auto sub : string_split(std::string(s1), 'd'))
    std::cout << sub << " ";
  std::cout << std::endl;

  return 0;
}