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
CXXFLAGS_PIE = -std=c++20 $(COMMON) -fpie

tls_bench_pic: misc-tls.cc
	$(CXX) $(CXXFLAGS_PIC) $(local-includes) $(INCLUDES) -DPIC misc-tls.cc -o tls_bench_pic

tls_bench_pie: misc-tls.cc
	$(CXX) $(CXXFLAGS_PIE) $(local-includes) $(INCLUDES) -DPIE misc-tls.cc -o tls_bench_pie

all: module

module: tls_bench_pic tls_bench_pie

clean:
	        rm tls_bench_pic tls_bench_pie
