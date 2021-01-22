let
  pkgs = import <nixpkgs> {};
  saga_albion = (import ./320-albion-saga-gis/default.nix) { inherit pkgs; };
in with pkgs;

stdenv.mkDerivation rec {
  name = "albion-solar-pv";
  version = "0.1";

  buildInputs = [
    (python37.withPackages (pps: with pps; [
      psycopg2
      requests
      gdal
      numpy
      scikitlearn
      scikitimage
    ]))
    saga_albion
  ];

  env = buildEnv {
    name = name;
    paths = buildInputs;
  };

  builder = builtins.toFile "builder.sh" ''
    source $stdenv/setup; ln -s $env $out
  '';
}