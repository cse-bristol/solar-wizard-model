#!/usr/bin/env bash

cd "$(dirname "$0")" || exit

while IFS= read -r coll; do
    phase=$(basename $(dirname "$coll"))
    outfile="scottish_lidar.txt"
    : > $outfile
    echo "writing filenames for phase $coll to $outfile"
    curl 'https://srsp-catalog.jncc.gov.uk/search/product' -X POST \
      -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0' \
      -H 'Accept: application/json' \
      -H 'Accept-Language: en-US,en;q=0.5' \
      -H 'Accept-Encoding: gzip, deflate, br' \
      -H 'Referer: https://remotesensingdata.gov.scot/' \
      -H 'Content-Type: application/json' \
      -H 'Origin: https://remotesensingdata.gov.scot' \
      -H 'Connection: keep-alive' \
      -H 'Sec-Fetch-Dest: empty' \
      -H 'Sec-Fetch-Mode: cors' \
      -H 'Sec-Fetch-Site: cross-site' \
      --data-raw '{"collections":["'"${coll}"'"],"footprint":"POLYGON((-8.152 54.4, -8.152 60.909, 0.088 60.909, 0.088 54.4, -8.152 54.4))","offset":0,"limit":700,"spatialop":"intersects"}' |
    # extract resource name and transform name -> url_path, e.g:
    # nh33_1m_dsm_phase1 -> /lidar/phase-1/dsm/27700/gridded/NH33_1M_DSM_PHASE1.tif
    jq -r --arg phase "$phase" \
      '.result | .[] | .name |  ascii_upcase | "https://srsp-open-data.s3.eu-west-2.amazonaws.com/lidar/" + $phase + "/dsm/27700/gridded/" + . + ".tif" | @text' >> "$outfile"
done < scottish_lidar_collections.txt

wget -i scottish_lidar.txt -T 10 --wait=10 --directory-prefix=/srv/lidar/scotland/