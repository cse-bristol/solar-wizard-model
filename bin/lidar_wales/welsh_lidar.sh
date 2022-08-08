#!/usr/bin/env bash

cd "$(dirname "$0")" || exit

PYTHONPATH=. python3 welsh_lidar.py

wget -i welsh_lidar.txt -T 10 --wait=10 --directory-prefix=/srv/lidar/wales/
