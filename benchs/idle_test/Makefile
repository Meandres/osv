src= ../..
include $(src)/modules/common.gmk

detect_arch=$(shell echo $(1) | $(CC) -E -xc- | tail -n 1)

ARCH = x64
MODE = release

local-includes = -I.
autodepend = -MD -MT $@ -MP

COMMON += -D __BSD_VISIBLE=1
COMMON += -include $(src)/compiler/include/intrinsics.hh
COMMON += -O3 -DNDEBUG -DCONF_debug_memory=0 -DOSV
COMMON += -fnon-call-exceptions -fasynchronous-unwind-tables
COMMON += -I $(src)/include

CXXFLAGS_PIC = -std=c++20 $(COMMON) -shared -fPIC -rdynamic

idle_test: idle_test.cc
	$(CXX) $(CXXFLAGS_PIC) $(local-includes) $(INCLUDES) idle_test.cc -o idle_test

all: module

module: idle_test

clean:
	        rm idle_test
