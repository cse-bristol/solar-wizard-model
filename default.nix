let
  pkgs = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/22.05.tar.gz") {});
  grass_pvmaps = pkgs.callPackage ./nix/grass-8.2.0-pvmaps.nix {};
in
pkgs.stdenv.mkDerivation rec {
  name = "albion-models";
  version = "0.1";

  buildInputs = [
    (pkgs.python310.withPackages (pps: [
      pps.psycopg2
      pps.requests
      pps.gdal
      pps.numpy
      pps.scikitlearn
      pps.scikitimage
      pps.shapely
    ]))
    pkgs.postgis
    pkgs.py-spy  # for profiling
    grass_pvmaps
  ];

  env = pkgs.buildEnv {
    name = name;
    paths = buildInputs;
  };

  builder = builtins.toFile "builder.sh" ''
    source $stdenv/setup; ln -s $env $out
  '';
}
