src = $(shell readlink -f ../..)
module-dir = $(src)/modules/jemalloc

all: module
module: jemalloc

jemalloc: upstream/jemalloc/.git upstream/jemalloc/lib/libjemalloc.so

.PHONY: jemalloc

upstream/jemalloc/.git:
	mkdir -p $(module-dir)/upstream && cd $(module-dir)/upstream && \
	git clone -b master --single-branch --depth 1 https://github.com/jemalloc/jemalloc.git
	# cd upstream/jemalloc; patch -p1 < patches/narenas.patch

upstream/jemalloc/lib/libjemalloc.so:
	cd $(module-dir)/upstream/jemalloc && ./autogen.sh --with-jemalloc-prefix=je_ && make

clean:
	cd $(module-dir) && rm -rf upstream
