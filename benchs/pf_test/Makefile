src= ../..
OSV_CXXFLAGS = -O3 -DNDEBUG -DCONF_debug_memory=0 -DOSV
OSV_CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
OSV_CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64

OSV_SRCS = pf_test.cc
OSV_OBJS = pf_test.o

module: pf_test

all: module

linux: pf_test.cc
	$(CXX) -O3 -DLINUX pf_test.cc -o pf_test_linux

# Rule to build the executable
pf_test: $(OSV_OBJS)
	$(CXX) $(OSV_CXXFLAGS) $^ -o $@

# Rule to compile source files
%.o: %.cc 
	$(CXX) $(OSV_CXXFLAGS) -c $< -o $@

# Clean target
clean:
	rm -f $(OSV_OBJS) $(OSV_EXEC)
