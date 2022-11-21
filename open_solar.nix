# For use with bin/open_solar.py via bin/open_solar

let
  pkgs = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/22.05.tar.gz") {});
  r_with_packages = pkgs.rWrapper.override {
    packages = with pkgs.rPackages; [
      rmapshaper
    ];
  };
in
pkgs.stdenv.mkDerivation rec {
  name = "albion-models";
  version = "0.1";

  buildInputs = [
    (pkgs.python310.withPackages (pps: [
      pps.psycopg2
      pps.requests
      pps.gdal
      pps.shapely
    ]))
    pkgs.tippecanoe
    pkgs.postgis
    r_with_packages
  ];

  env = pkgs.buildEnv {
    name = name;
    paths = buildInputs;
  };

  builder = builtins.toFile "builder.sh" ''
    source $stdenv/setup; ln -s $env $out
  '';
}
