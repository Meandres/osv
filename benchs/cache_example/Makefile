src= ../..
CXXFLAGS = -g -DNDEBUG -DCONF_debug_memory=0 -std=c++20 -DOSV #-O3
CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64 -mavx -mavx2

cache_example: cache_example.cpp
	$(CXX) $(CXXFLAGS) cache_example.cpp -o cache_example


all: module

module: cache_example

clean:
	        rm cache_example
