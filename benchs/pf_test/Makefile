src= ../..
CXXFLAGS = -O3 -DNDEBUG -DCONF_debug_memory=0
CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64

SRCS = pf_test.cc
OBJS = pf_test.o

module: pf_test

all: module

# Rule to build the executable
pf_test: $(OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@

# Rule to compile source files
%.o: %.cc 
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean target
clean:
	rm -f $(OBJS) $(EXEC)
