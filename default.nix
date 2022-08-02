let
  pkgs = import <nixpkgs> {};
in with pkgs;
# let
#   pg = postgresql_12;
#   pgis = postgis.override { postgresql = pg; };
# in
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
