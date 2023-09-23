#!/bin/bash
# Procedure to sanity-check kioperf binary after build

if [ "${KIOHOME}" = "" ]; then
  KIOHOME=$(cd `dirname $0`/.. ; pwd)
fi
if [ "${KIOPERF}" = "" ]; then
  KIOPERF=$KIOHOME/kioperf
fi
echo "KIOHOME: $KIOHOME"
echo "KIOPERF: $KIOPERF"

set -ex
mkdir -p $KIOHOME/test/kioperf-data
cd $KIOHOME/test
$KIOPERF help
$KIOPERF disk --operation=write --threads=3 --iterations=6 --files=6 --size=1
$KIOPERF disk --operation=read --threads=3 --iterations=12 --files=6 
