with import <nixpkgs> {};

let
    solar_pv = callPackage ./build.nix {};
in python37.withPackages (ps: [ solar_pv ] )
