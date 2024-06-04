src= ../..
CXXFLAGS = -O3 -g -DNDEBUG -DCONF_debug_memory=0 -std=c++20
CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64 -mavx -mavx2

module: tls_bench

all: module
tls_bench: misc-tls.cc
	g++ $(CXXFLAGS) misc-tls.cc -o tls_bench

clean:
	rm tls_bench

