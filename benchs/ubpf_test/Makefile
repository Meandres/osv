.PHONY: module
module: ubpf_test ebpf_prog

OSV_SRC=../..
INCLUDES= -I$(OSV_SRC) -I$(OSV_SRC)/modules/ubpf/vm/inc -I$(OSV_SRC)/modules/ubpf/build/vm/ -I$(OSV_SRC)/modules/ubpf/ -I$(OSV_SRC)/include -I$(OSV_SRC)/arch -I$(OSV_SRC)/arch/x64 -I $(OSV_SRC)/arch/common -I.
UBPF_LINK=-L../../modules/ubpf/build/lib -lubpf
helpers= io.o
CXXFLAGS= -shared -fPIC -DNDEBUG -DCONF_debug_memory=0 -DOSV

ubpf_test: ubpf_test.cc $(helpers)
	g++ $(CXXFLAGS) $(INCLUDES) ubpf_test.cc $(UBPF_LINK) $(helpers) -o ubpf_test

ebpf_prog: ebpf_prog.c
	clang -O2 -target bpf -c ebpf_prog.c -o ebpf_prog.o

%.o: %.cc
	g++ $(INCLUDES) -c -o $@ $<

clean:
	rm ubpf_test
