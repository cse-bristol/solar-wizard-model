{ callPackage, lib, stdenv, fetchFromGitHub, flex, bison, pkg-config, zlib, libtiff, libpng, fftw
, cairo, readline, ffmpeg, makeWrapper, wxGTK31, wxmac, netcdf, blas
, gdal, geos, sqlite, postgresql, libmysqlclient, python3Packages, libLAS
, zstd, pdal, wrapGAppsHook, glibcLocales
}:

let
  grass-package = with lib; fetchFromGitHub {
        owner = "OSGeo";
        repo = "grass";
        rev = "8.2.0";  # If changing, fix "grass82" in substituteInPlace below
        sha256 = "1phdwcjh1qz54di10gas5nq9q0d4lmfr6rgf4ng6j71hl854bbsl";
  };
in
stdenv.mkDerivation rec {
  pname = "grass_pvmaps";
  version = "0.0.0";
  
  doCheck = false;  # Set to true to run the tests after building
  
  # Using proj 4.9.3 (i.e. version < 5) causes grass to work with the "old" proj api used by pvmaps - so that they both work
  proj_4_9_3 = callPackage ./proj-4.9.3.nix {};
  
  nativeBuildInputs = [ pkg-config ];
  
  buildInputs = [ flex bison zlib proj_4_9_3 gdal libtiff libpng fftw sqlite
        readline ffmpeg makeWrapper netcdf geos postgresql libmysqlclient blas
        libLAS zstd wrapGAppsHook cairo pdal wxGTK31 glibcLocales
        (with python3Packages; [ python python-dateutil numpy wxPython_4_1 ])
  ];
  
  configureFlags = [
    "--with-proj-share=${proj_4_9_3}/share/proj"
    "--with-proj-includes=${proj_4_9_3.dev}/include"
    "--with-proj-libs=${proj_4_9_3}/lib"
    "--without-opengl"
    "--with-readline"
    "--with-wxwidgets"
    "--with-netcdf"
    "--with-geos"
    "--with-postgres"
    "--with-postgres-libs=${postgresql.lib}/lib/"
    "--with-mysql"
    "--with-mysql-includes=${lib.getDev libmysqlclient}/include/mysql"
    "--with-mysql-libs=${libmysqlclient}/lib/mysql"
    "--with-blas"
    "--with-liblas=${libLAS}/bin/liblas-config"
    "--with-zstd"
    "--with-fftw"
    "--with-pthread"
    "--with-pdal"
  ];
  
  # Otherwise a very confusing "Can't load GDAL library" error
  makeFlags = lib.optional stdenv.isDarwin "GDAL_DYNAMIC=";
  
  enableParallelBuilding = true;
  
  sourceRoot = "source";

  
  unpackPhase = ''
    # Using "srcs" doesn't work as the grass package is readonly 
    # so the pvgis code can't be unpacked over it - so:

    echo Unpacking grass
    cp -r ${grass-package} ./source
    
    # Change mode of dirs so that below works but also the configure logfile can be written
    find . -type d -exec chmod u=rwx {} \;

    cd source/raster

    # bring in r.pv:
    cp -r ${../grass_modules/r.pv} ./r.pv
    chmod u=rwx ./r.pv

    # bring in r.horizonmask:
    cp -r ${../grass_modules/r.horizonmask} ./r.horizonmask
    chmod u=rwx ./r.horizonmask

    cd ../.. 
  '';
  
  
  patchPhase = ''
      # Correct mysql_config query
      substituteInPlace configure --replace "--libmysqld-libs" "--libs"
      
      # Add building the pvgis exes
      substituteInPlace raster/Makefile --replace "SUBDIRS = " "SUBDIRS = r.pv r.horizonmask "
      
      # Fix r.sun north facing panels issues
      cd raster
      chmod -R u=rwx r.sun
      patch -p0 < ${./r.sun.patch}
      cd ..

      # Fix r.pv north facing panels issues (it uses a copy of the code from r.sun)
      cd raster
      chmod -R u=rwx r.pv
      patch -p0 < ${./r.pv.patch}
      cd ..
  '';  
  
  
  postConfigure = ''
    # Ensure that the python script run at build time are actually executable;
    # otherwise, patchShebangs ignores them.
    for f in $(find . -name '*.py'); do
      chmod +x $f
    done

    # See https://discourse.nixos.org/t/what-is-the-patchshebangs-command-in-nix-build-expressions/12656#32-explicitly-6
    # i.e. looks like this means "patch shebangs in all dirs under the source dir"
    patchShebangs */
  '';

  postInstall = ''
    wrapProgram $out/bin/grass \
        --set PYTHONPATH $PYTHONPATH \
        --set GRASS_PYTHON ${python3Packages.python.interpreter} \
        --suffix LD_LIBRARY_PATH ':' '${gdal}/lib'
    ln -s $out/grass*/lib $out/lib
    ln -s $out/grass*/include $out/include
  '';
}

