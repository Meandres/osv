{
  description = "OSv flake";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs?ref=23.11";
    nixpkgs-2211.url = "github:nixos/nixpkgs?ref=22.11";
  };

  outputs = { self, nixpkgs, nixpkgs-2211, flake-utils, }@inputs:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ (import ./overlays.nix { inherit inputs; }) ];
          };
          #glibc-2-38 = inputs.nixpkgs-2311.legacyPackages.${pkgs.system}.glibc.all;
        in
        {
          devShell = pkgs.mkShell {

            nativeBuildInputs = with pkgs; [
              ack # grep tool
              ant # java dev lib
              autoconf
              automake
              bash
              binutils
              bisoncpp
              gcc13
              gdb # gnu debugger
              cmake
              gnumake
              gnupatch
              flamegraph # code hierarchy visualization
              libedit
              libgcc # Compiler
              libtool
              libvirt
              lua53Packages.lua
              ncurses
              pax-utils # elf security library
              python3
              python311Packages.requests
              p11-kit # PKCS#11 loader
              qemu_full # hypervisor
              readline # interactive line editing
              unzip
              zulu8 # Java jdk
              clang
              osv-ssl
              osv-ssl-hdr
              yaml-cpp
              xz.out
              krb5.out
              libselinux.out
              libz
              boost175
              unixODBC
              numactl
              python311Packages.numpy
              python311Packages.pandas
              python311Packages.matplotlib
              virtiofsd
              just
              flex
              bison
              ninja
              tbb
              dpdk
              snappy
              zstd
              zlib
              bzip2
              curl
              glog
              lz4
              openssl
            ];

            buildInputs = with pkgs; [
              osv-boost # C++ libraries
              readline # interactive line editing
              libaio # I/O library
              osv-ssl # SSL/TLS library
              clang-tools # language server
            ];

            LD_LIBRARY_PATH = "${pkgs.readline}/lib";
            LUA_LIB_PATH = "${pkgs.lua53Packages.lua}/lib";
            GOMP_DIR = pkgs.libgcc.out;
            STATIC_LIBC= pkgs.glibc.static;
            boost_base = "${pkgs.osv-boost}";
            BOOST_SO_DIR="${pkgs.boost175}/lib";
            OPENSSL_DIR="${pkgs.osv-ssl}";
            OPENSSL_HDR="${pkgs.osv-ssl-hdr}/include";
            KRB5_DIR="${pkgs.krb5.out}";
            XZ_DIR="${pkgs.xz.out}";
            LIBZ_DIR="${pkgs.libz}";
            LIBSELINUX_DIR="${pkgs.libselinux.out}";
            DPDK_DIR="${pkgs.dpdk}";

            CAPSTAN_QEMU_PATH = "${pkgs.qemu}/bin/qemu-system-x86_64";

            /*shellHook = ''
              mkdir $TMP/openssl-all
              ln -rsf ${pkgs.openssl}/* $TMP/openssl-all
              ln -rsf ${pkgs.openssl.dev}/* $TMP/openssl-all
              ln -rsf ${pkgs.openssl.out}/* $TMP/openssl-all
              export OPENSSL_DIR="$TMP/openssl-all";
              export OPENSSL_LIB_PATH="$TMP/openssl-all/lib";
            '';*/
          };
        }
      );
}
              #/bin/bash --version >/dev/null 2>&1 || {
               # echo >&2 "Error: /bin/bash is required but was not found.  Aborting."
                #echo >&2 "If you're on NixOs, consider using https://github.com/Mic92/envfs."
                #exit 1
                #}

              #mkdir $TMP/libboost
              #ln -s ${pkgs.osv-boost}/lib/* $TMP/libboost/
              #for file in $TMP/libboost/*-x64*; do mv "$file" "''${file//-x64/}"; done
              #export boost_base="$TMP/libboost"

