{ compiler ? "ghc844"
, ...
}:
let
  config = {
    allowBroken = true;
    packageOverrides = pkgs: rec {
      haskellPackages = pkgs.haskellPackages.override {
        overrides = haskellPackagesNew: haskellPackagesOld: rec {
          postgresql-simple-url = pkgs.haskell.lib.dontCheck haskellPackagesOld.postgresql-simple-url;
          dispenser-core = haskellPackagesNew.callPackage ../dispenser-core/dispenser-core.nix {};
        };
      };
    };
  };
  pkgs = import <nixpkgs> { inherit config; };
  haskellPackages = pkgs.haskell.packages.${compiler};
  hs = pkgs.haskellPackages;
in
pkgs.haskell.lib.dontCheck (hs.callPackage ./dispenser-server.nix {})
