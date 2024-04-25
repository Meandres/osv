src= ../..
#include $(src)/modules/common.gmk

#detect_arch=$(shell echo $(1) | $(CC) -E -xc- | tail -n 1)

ARCH = x64 
ifneq ($(OSV_BUILD_PATH),)
mode := $(notdir $(OSV_BUILD_PATH))
endif

local-includes = -I.
#autodepend = -MD -MT $@ -MP

#COMMON += -D __BSD_VISIBLE=1
#COMMON += -include $(src)/compiler/include/intrinsics.hh
COMMON += -O3 -DNDEBUG -DCONF_debug_memory=0
#COMMON += -g -DOSV -DCONF_debug_memory=0
#COMMON += -fnon-call-exceptions -fasynchronous-unwind-tables
COMMON += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/$(ARCH)

CXXFLAGS = -std=c++20 $(COMMON) -shared -fPIC -rdynamic
#CXXFLAGS = -std=c++20 $(COMMON) #-fpie #-rdynamic
#LDFLAGS += $(src)/build/$(mode)/libosv.so

dbmockup_explicit: load_injector.cpp tpcc/* BTree.hpp
	$(CXX) $(CXXFLAGS) $(local-includes) $(INCLUDES) -DEXPLICIT load_injector.cpp -o dbmockup_ex

dbmockup_modular: load_injector.cpp tpcc/* BTree.hpp
	$(CXX) $(CXXFLAGS) $(local-includes) $(INCLUDES) -DMODULAR load_injector.cpp -o dbmockup_mod

all: module

module: dbmockup_modular dbmockup_explicit 

clean:
	        rm dbmockup_ex dbmockup_mmap dbmockup_mod
