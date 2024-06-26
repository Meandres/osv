ROOT_DIR := $(shell pwd)
CLI_ROOT=$(ROOT_DIR)/..
LUA_DIR=$(CLI_ROOT)/../lua
VERSION=1.0

MAIN=rpm
CLI_DIR=osv-cli-$(VERSION)/usr/lib64/osv-cli
if [[ -z "$(LUA_LIB_PATH)" ]]; then
	LUA_LIB_PATH = $(shell ldconfig -p | grep -Po "/.*liblua*.5\.3.so" | head -1)
fi


module: clean all

all: $(MAIN)

init:
	mkdir -p BUILD
	mkdir -p BUILDROOT
	mkdir -p $(CLI_DIR)/lib
	mkdir -p RPMS
	mkdir -p SOURCES
	mkdir -p SRPMS
	echo $(ROOT_DIR)

$(MAIN): init copy-files
	tar -zcvf osv-cli-1.0.tar.gz osv-cli-1.0/
	mv osv-cli-1.0.tar.gz SOURCES/
	rpmbuild --define "_topdir `pwd`" -ba SPECS/osv-cli.spec

copy-files:
	cp $(CLI_ROOT)/cli $(CLI_DIR)
	cp $(CLI_ROOT)/cli.lua $(CLI_DIR)
	cp $(CLI_ROOT)/lib/*.lua $(CLI_DIR)/lib
	cp -r $(CLI_ROOT)/commands $(CLI_DIR)
	cp -r $(LUA_DIR)/install/lua_modules/share $(CLI_DIR)/lib
	cp -r $(LUA_DIR)/install/lua_modules/lib $(CLI_DIR)/lib
	cp $(LUA_LIB_PATH) $(CLI_DIR)/lib

clean:
	rm -rf BUILD
	rm -rf BUILDROOT
	rm -rf $(CLI_DIR)
	rm -rf RPMS
	rm -rf SOURCES
	rm -rf SRPMS

.PHONY: module clean
