{ inputs, ... }:

final: _prev: {
  capstan = _prev.callPackage ./pkgs/capstan.nix { };
  osv-boost = _prev.boost.override { enableStatic = true; enableShared = false; };
}
