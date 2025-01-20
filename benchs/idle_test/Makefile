src= ../..
CXXFLAGS = -O3 -g -DNDEBUG -DCONF_debug_memory=0 -std=c++20
CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64

module: 
	g++ $(CXXFLAGS) -o idle_test idle_test.cc

all: module

clean:
	        rm idle_test
