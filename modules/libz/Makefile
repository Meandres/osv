# Take libz.so from the build machine and put it in the image.

very-quiet = $(if $V, $1, @$1)

# Detect if running on NixOS
NIXOS = $(shell test -e /etc/NIXOS && echo 1 || echo 0)

# Find libz dynamically
ifeq ($(NIXOS), 1)
    libz = $(shell nix eval --raw nixpkgs#zlib)/lib/libz.so.1
else
    libz = $(shell $(CC) -print-file-name=libz.so.1)
endif

module:
	$(call very-quiet, echo /usr/lib/libz.so.1: $(libz) > usr.manifest)

