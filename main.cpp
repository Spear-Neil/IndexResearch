#include <iostream>
#include <typeinfo>

int main() {
  const std::type_info& info = typeid(int*);
  std::cout << info.name() << std::endl;
  const std::type_info& inf1 = typeid(size_t*);
  std::cout << inf1.name() << std::endl;
}
