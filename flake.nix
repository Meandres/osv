{
  description = "OSv flake";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs?ref=23.11";
    nixpkgs-2211.url = "github:nixos/nixpkgs?ref=22.11";
    nur-niwa.url = "github:Meandres/nur-niwa";
  };

  outputs = { self, nixpkgs, nixpkgs-2211, flake-utils, nur-niwa }@inputs:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ (import ./overlays.nix { inherit inputs; }) ];
          };
          niwa-pkgs = nur-niwa.packages.x86_64-linux;
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
              niwa-pkgs.driverctl
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
          };
        }
      );
}

