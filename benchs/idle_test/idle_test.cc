#include <iostream>
#include <chrono>
#include <thread>
#include <osv/cache.hh>
#include <drivers/nvme.hh>
#include <bitset>
#include <sys/mman.h>

using namespace std;

int main()
{
  void* mem = mmap(NULL, 4*4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
  mem = mem+4096;
  memset(mem, 0, 2*4096);
  testpair expected, desired;
  expected.low = walk(mem).word;
  expected.high = walk(mem+4096).word;
  desired.low = 0ull;
  desired.high = 0ull;
  testpair* ptr = reinterpret_cast<testpair*>(walkRef(mem));

  std::cout << "Before: " << std::hex << walk(mem).word << walk(mem+4096).word << "\n";
  bool result = test_CAS_dw(ptr, expected, desired);
  std::cout << "CAS result: " << result << "\n";
  std::cout << "After:  " << std::hex << walk(mem).word << walk(mem+4096).word << "\n";
  return 0;
}
