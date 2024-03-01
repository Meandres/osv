{ pkgs ? import <nixpkgs> {} }:
let 
  osv-boost = pkgs.boost175.override{ enableStatic=true; enableShared = false;};
in
  pkgs.stdenv.mkDerivation {
        name="osv-dev-env";
        nativeBuildInputs = with pkgs.buildPackages; [ gnumake unzip readline docker pax-utils pkg-config lua53Packages.lua qemu_full ];
        #buildInputs = with pkgs.buildPackages; [ (boost.override { enableStatic = true; enableShared = false; }) readline libaio openssl.out openssl.dev openssl ];
        buildInputs = with pkgs.buildPackages; [ osv-boost readline libaio openssl.out openssl.dev openssl ];
	boost_base = osv-boost;
	shellHook = ''
		export LD_LIBRARY_PATH=$(nix eval --raw nixpkgs#readline)/lib

		export LUA_LIB_PATH=$(nix eval --raw nixpkgs#lua53Packages.lua)/lib

		sudo rm -rf /tmp/openssl-all
		mkdir /tmp/openssl-all
    		ln -rsf $(nix eval --raw nixpkgs#openssl)/* /tmp/openssl-all
    		ln -rsf $(nix eval --raw nixpkgs#openssl.dev)/* /tmp/openssl-all
    		ln -rsf $(nix eval --raw nixpkgs#openssl.out)/* /tmp/openssl-all
    		ln -rsf $(nix eval --raw nixpkgs#openssl.out)/* /tmp/openssl-all

		export OPENSSL_DIR=/tmp/openssl-all
		export OPENSSL_LIB_PATH=/tmp/openssl-all/lib
  	'';
}
