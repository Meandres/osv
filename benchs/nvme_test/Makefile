.PHONY: module
module: ioengine_unvme

OSV_SRC=../..
INCLUDES= -I$(OSV_SRC) -I$(OSV_SRC)/include -I$(OSV_SRC)/arch -I$(OSV_SRC)/arch/x64 -I $(OSV_SRC)/arch/common -I. -I $(OSV_SRC)/apps
CXXFLAGS= -shared -fPIC -DNDEBUG -DCONF_debug_memory=0 -DOSV

ioengine_unvme: ioengine_unvme.c
	g++ $(CXXFLAGS) $(INCLUDES) ioengine_unvme.c -o ioengine_unvme

clean:
	rm ioengine_unvme
