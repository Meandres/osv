src= ../..
OSV_CXXFLAGS = -O3 -DNDEBUG -DCONF_debug_memory=0 -DOSV
OSV_CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
OSV_CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64

OSV_SRCS = tlb_test.cc
OSV_OBJS = tlb_test.o

module: tlb_test

all: module

# Rule to build the executable
tlb_test: $(OSV_OBJS)
	$(CXX) $(OSV_CXXFLAGS) $^ -o $@

# Rule to compile source files
%.o: %.cc 
	$(CXX) $(OSV_CXXFLAGS) -c $< -o $@

# Clean target
clean:
	rm -f $(OSV_OBJS) $(OSV_EXEC)
