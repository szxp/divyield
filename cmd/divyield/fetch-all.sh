#/bin/sh

# set token
#   export IEXTOKEN=<token>

go build && ./divyield.exe fetch \
    --iexCloudAPIToken=$IEXTOKEN \
    `ls work/stocks`
