#!/bin/sh

#set -x

go build cmd/divyield/main.go && \
    ./main.exe \
    stats \
    -start-date=-5y \
    $@ \
    ALC \
    BAYZF \
    BAMXF \
    CVGW \
    KO \
    ENAKF \
    HLBZF \
    NSRGF \
    NVSEF \
    PEP \
    

