{ lib
, stdenv
, fetchFromGitHub
, autoreconfHook
, pkg-config
, buildPackages
, sqlite
, libtiff
, curl
}:

stdenv.mkDerivation rec {
  pname = "proj";
  version = "4.9.3";

  doCheck = false;  # Set to true to run the tests after building

  src = fetchFromGitHub {
    owner = "OSGeo";
    repo = "PROJ";
    rev = version;
    sha256 = "1a5i3w0r0yqsw5wp9xzg2r241ncsh8s7rn7znfid2nvjzwg53iis";
  };

  outputs = [ "out" "dev" ];

  nativeBuildInputs = [ autoreconfHook pkg-config ];

  buildInputs = [ sqlite libtiff curl ];
}
