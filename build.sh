#!/bin/bash
set -xeuo pipefail
yum -y install qemu-img rsync && yum clean all
