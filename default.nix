# For development

let
  pkgs = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/b18a4b9.tar.gz") {});

  # GRASS and pvmaps pinned to 22.05:
  pkgs2205 = (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/22.05.tar.gz") {});

  python = pkgs.python312;

  # Option to disable the (slow) GRASS build (set NIX_BUILD_GRASS=false to disable):
  buildGrass = builtins.getEnv "NIX_BUILD_GRASS" != "false";
  grass_pvmaps = pkgs2205.callPackage ./nix/grass-8.2.0-pvmaps.nix {};
in
pkgs.mkShell {
  name = "solar-wizard-model";

  buildInputs = [
    python
    pkgs.gdal
    (pkgs.postgresql_17.withPackages (p: [ p.postgis ]))
    pkgs.postgresql_17.pg_config
    pkgs.py-spy
  ] ++ pkgs.lib.optional buildGrass grass_pvmaps;

  # numpy / scikit-learn / scikit-image etc. install as manylinux wheels that are
  # linked against a standard glibc + libstdc++:
  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc pkgs.zlib ];

  shellHook = ''
    d="${toString ./.}"
    python_bin="${python}/bin/python"

    # Only (re)install when either requirements file or the interpreter changes:
    req_sum="$(cksum "$d/requirements.txt" "$d/requirements-dev.txt") $python_bin"

    if [ "$(cat "$d/.venv/.venv.cksum" 2>/dev/null)" != "$req_sum" ]; then
      echo "installing solar-wizard-model python dependencies ..."

      rm -rf "$d/.venv"
      "$python_bin" -m venv "$d/.venv" || { echo "failed to create .venv" >&2; return 1; }
      source "$d/.venv/bin/activate"

      python -m pip install --ignore-installed --quiet "pip==26.1" || { echo "failed to install pip==26.1" >&2; return 1; }

      # GDAL's setup.py imports numpy at build time but doesn't declare it as a build dependency:
      numpy_pin="$(grep -ihE '^numpy==' "$d/requirements.txt")"
      python -m pip install -q setuptools wheel "$numpy_pin" \
        || { echo "failed to install build deps" >&2; return 1; }

      # Make numpy's headers findable via the compiler's env rather than only the -I
      # GDAL's setup.py passes: nix's cc-wrapper strips -I paths outside the store under
      # purity enforcement (as on the CI runners), which otherwise breaks the gdal_array build.
      export CPLUS_INCLUDE_PATH="$(python -c 'import numpy; print(numpy.get_include())')''${CPLUS_INCLUDE_PATH:+:$CPLUS_INCLUDE_PATH}"

      # Don't install versions released in the last 7 days, to reduce risk of supply chain attacks:
      python -m pip install -r "$d/requirements.txt" -q --no-build-isolation --uploaded-prior-to=P7D \
        || { echo "failed to install requirements.txt (see error above)" >&2; return 1; }

      # install the model itself in editable mode; its deps are already satisfied above,
      # so --no-deps keeps pip from re-resolving them:
      python -m pip install --no-deps -e "$d" \
        || { echo "failed to install solar-wizard-model" >&2; return 1; }

      # test-only deps:
      python -m pip install -r "$d/requirements-dev.txt" -q --uploaded-prior-to=P7D \
        || { echo "failed to install requirements-dev.txt (see error above)" >&2; return 1; }

      echo "$req_sum" > "$d/.venv/.venv.cksum"
    else
      source "$d/.venv/bin/activate"
    fi
  '';
}
