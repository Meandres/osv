#
# Copyright (C) 2019 Waldemar Kozaczuk, Ltd.
#
# This work is open source software, licensed under the terms of the
# BSD license as described in the LICENSE file in the top-level directory.
#
SRC = $(shell readlink -f ../..)

RUNNING_IN_NIXOS= $(shell uname -v | grep NixOS)

module:
		# THIS WORKS ONLY IN NIXOS
		echo "/usr/lib/libssl.so.1.1: $(OPENSSL_DIR)/lib/libssl.so.1.1" > usr.manifest
		echo "/usr/lib/libgssapi_krb5.so.2: $(KRB5_DIR)/lib/libgssapi_krb5.so.2" > usr.manifest
		echo "/usr/lib/libselinux.so.1: $(SELINUX_DIR)/lib/libselinux.so" > usr.manifest
		echo "/usr/lib/liblzma.so.5: $(XZ_DIR)/lib/liblzma.so.5" > usr.manifest
		# $(SRC)/scripts/manifest_from_host.sh -l libssl.so.1.1 > usr.manifest
		# From krb5-libs
		# $(SRC)/scripts/manifest_from_host.sh -l libgssapi_krb5.so.2 >> usr.manifest
		# From libselinux
		# $(SRC)/scripts/manifest_from_host.sh -l libselinux.so.1 >> usr.manifest
		# From xz-libs
		# $(SRC)/scripts/manifest_from_host.sh -l liblzma.so.5 >> usr.manifest

.PHONY: module

clean:
	rm -rf usr.manifest
.PHONY: clean
