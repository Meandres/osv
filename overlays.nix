{ inputs, ... }:

final: _prev: {
  capstan = _prev.callPackage ./pkgs/capstan.nix { };
  osv-boost = _prev.boost175.override { enableStatic = true; enableShared = false; };
  osv-ssl = inputs.nixpkgs-2211.legacyPackages.${_prev.system}.openssl_1_1.out;
  osv-ssl-hdr = inputs.nixpkgs-2211.legacyPackages.${_prev.system}.openssl_1_1.dev;
}
