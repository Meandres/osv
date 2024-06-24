src= ../..
CXXFLAGS = -g -O3 -DNDEBUG -DCONF_debug_memory=0 -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared -DOSV
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64  -mavx -mavx2

iotest: io_test.cc
	$(CXX) $(CXXFLAGS) io_test.cc -o io_test

module: io_test

clean:
	        rm io_test
