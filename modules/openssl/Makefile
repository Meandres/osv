#
# Copyright (C) 2019 Waldemar Kozaczuk, Ltd.
#
# This work is open source software, licensed under the terms of the
# BSD license as described in the LICENSE file in the top-level directory.
#
SRC = $(shell readlink -f ../..)

module:
ifneq ($(OPENSSL_DIR),"")
	echo "/libssl.so.1.1: $(OPENSSL_DIR)/lib/libssl.so.1.1" > usr.manifest
	echo "/libcrypto.so.1.1: $(OPENSSL_DIR)/lib/libcrypto.so.1.1" >> usr.manifest
else
	$(SRC)/scripts/manifest_from_host.sh -l libssl.so.1.1 > usr.manifest
endif
ifneq ($(KRB5_DIR),"")
	echo "/libgssapi_krb5.so.2: $(KRB5_DIR)/lib/libgssapi_krb5.so.2" >> usr.manifest
else
	# From krb5-libs
	$(SRC)/scripts/manifest_from_host.sh -l libgssapi_krb5.so.2 >> usr.manifest
endif
ifneq ($(LIBSELINUX_DIR),"")
	echo "/libselinux.so.1: $(LIBSELINUX_DIR)/lib/libselinux.so.1" >> usr.manifest
else
	# From libselinux
	$(SRC)/scripts/manifest_from_host.sh -l libselinux.so.1 >> usr.manifest
endif
ifneq ($(XZ_DIR),"")
	echo "/liblzma.so.5: $(XZ_DIR)/lib/liblzma.so.5" >> usr.manifest
else
	# From xz-libs
	$(SRC)/scripts/manifest_from_host.sh -l liblzma.so.5 >> usr.manifest
endif

.PHONY: module

clean:
	rm -rf usr.manifest
.PHONY: clean
