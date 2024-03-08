{
    description = "DevShell to build OSv";

    inputs = {
        nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    };

    outputs = { self, nixpkgs }: {
        let {
            pkgs = import nixpkgs { system = "x86_64-linux"; };
            osv-boost = pkgs.boost175.override{ enableStatic=true; enableShared = false;};
            makeOSvShell = pkgs.stdenv.mkDerivation {
                nativeBuildInputs = with pkgs.buildPackages; [ gnumake unzip readline docker pax-utils pkg-config lua53Packages.lua qemu_full gdb ack bash python3 ];
                buildInputs = with pkgs.buildPackages; [ osv-boost readline libaio openssl.out openssl.dev openssl ];
	            boost_base = osv-boost;
	            shellHook = ''
		            export LD_LIBRARY_PATH=$(nix eval --raw nixpkgs#readline)/lib
    
		            export LUA_LIB_PATH=$(nix eval --raw nixpkgs#lua53Packages.lua)/lib

		            mkdir -p /tmp/openssl-all
    		        ln -rsf $(nix eval --raw nixpkgs#openssl)/* /tmp/openssl-all
    		        ln -rsf $(nix eval --raw nixpkgs#openssl.dev)/* /tmp/openssl-all
    		        ln -rsf $(nix eval --raw nixpkgs#openssl.out)/* /tmp/openssl-all
    		        ln -rsf $(nix eval --raw nixpkgs#openssl.out)/* /tmp/openssl-all

		            export OPENSSL_DIR=/tmp/openssl-all
		            export OPENSSL_LIB_PATH=/tmp/openssl-all/lib
  	            '';
            };
        } in {
            devShell.x86_64-linux = makeOSvShell;
        };
    };
}

