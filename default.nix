let
  pkgs = import <nixpkgs> {};
in with pkgs;
stdenv.mkDerivation rec {
  name = "albion-models";
  version = "0.1";

  buildInputs = [
    (python37.withPackages (pps: with pps; [
      psycopg2
      requests
      gdal
      numpy
      scikitlearn
      scikitimage
      shapely
    ]))
    postgis
  ];

  env = buildEnv {
    name = name;
    paths = buildInputs;
  };

  builder = builtins.toFile "builder.sh" ''
    source $stdenv/setup; ln -s $env $out
  '';
}
