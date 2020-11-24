{ nixpkgs ? import <nixpkgs> {}, python37Packages ? nixpkgs.pkgs.python37Packages }:

with nixpkgs;
python37Packages.buildPythonPackage rec {
  pname = "albion_solar_pv";
  version = "0.1";

  src = ./.;

  propagatedNativeBuildInputs = [
    python37Packages.psycopg2
    python37Packages.requests
    python37Packages.gdal
    python37Packages.numpy
    (pkgs.callPackage ./320-albion-saga-gis/default.nix {})
  ];

}
