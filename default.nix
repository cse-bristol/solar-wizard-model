# For development

let
  pkgs = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/22.05.tar.gz") {});
  
  # Option to disable GRASS build (set NIX_BUILD_GRASS=false to disable)
  buildGrass = builtins.getEnv "NIX_BUILD_GRASS" != "false";
  
  grass_pvmaps = if buildGrass then pkgs.callPackage ./nix/grass-8.2.0-pvmaps.nix {} else null;
  
  shapely = pkgs.python310.pkgs.buildPythonPackage rec {
    pname = "shapely";
    version = "2.0.1";
    src = pkgs.python310.pkgs.fetchPypi {
      inherit pname version;
      hash = "sha256:14v88k0y7qhp8n5clip6w96pkdzrfqa2hsjkhpy9gkifwyiv39k6";
    };
    nativeBuildInputs = [ pkgs.python310.pkgs.cython pkgs.geos pkgs.python310.pkgs.setuptools ];
    buildInputs = [ pkgs.geos ];
    propagatedBuildInputs = [ pkgs.python310.pkgs.numpy ];
    doCheck = false;
  };

  baseBuildInputs = [
    (pkgs.python310.withPackages (pps: [
      pps.psycopg2
      pps.requests
      pps.gdal
      pps.numpy
      pps.scikitlearn
      pps.scikitimage
      shapely
    ]))
    pkgs.postgis
    pkgs.py-spy  # for profiling
  ];
in
pkgs.stdenv.mkDerivation rec {
  name = "solar-wizard-model";
  version = "0.1";

  buildInputs = baseBuildInputs ++ (if buildGrass then [ grass_pvmaps ] else []);

  env = pkgs.buildEnv {
    name = name;
    paths = buildInputs;
  };

  builder = builtins.toFile "builder.sh" ''
    source $stdenv/setup; ln -s $env $out
  '';
}
