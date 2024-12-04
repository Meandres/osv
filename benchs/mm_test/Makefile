linux-mmtest:
	g++ -std=c++20 -mavx -mavx2 -O3 mm_test.cc -o linux_mm_test -DLINUX

osv-mmtest:
src= ../..
CXXFLAGS = -g -O3 -DNDEBUG -DCONF_debug_memory=0 -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared -DOSV
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x86 -mavx -mavx2

osv-mmtest: mm_test.cc
	$(CXX) $(CXXFLAGS) mm_test.cc -o mm_test

all: linux-mmtest

module: osv-mmtest

clean:
	        rm mm_test
