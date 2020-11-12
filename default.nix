with import<nixpkgs> {};

stdenv.mkDerivation rec {
  name = "albion-webapp";
  version = 0.1;

  buildInputs = [
    (python37.withPackages (pps: with pps; [
      psycopg2
      requests
      gdal
    ]))
  ];

  env = buildEnv {
    name = name;
    paths = buildInputs;
  };

  builder = builtins.toFile "builder.sh" ''
    source $stdenv/setup; ln -s $env $out
  '';
}
