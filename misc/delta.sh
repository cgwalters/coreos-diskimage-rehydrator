#!/bin/bash
set -xeuo pipefail

cmd=$1
shift
case $cmd in
  dehydrate)
    src=$1; shift
    dest=$1; shift
    out=$1; shift
    ostree --repo=deltarepo init --mode=bare-user-only
    mkdir deltarepo/tmp/t
    cp --reflink=auto $src deltarepo/tmp/t
    ostree --repo=deltarepo commit -b delta --tree=dir=deltarepo/tmp/t --consume
    cp --reflink=auto $dest deltarepo/tmp/t
    ostree --repo=deltarepo commit -b delta --tree=dir=deltarepo/tmp/t --consume
    ostree --repo=deltarepo static-delta generate delta --tree=dir=deltarepo/tmp/t --consume
    

   
  rehydrate)

  *) echo "unknown command: $1"; exit 1;;
esac
src=$1
