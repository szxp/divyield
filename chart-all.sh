#/bin/sh

go build && ./divyield.exe fetch `ls work/stocks` && \
go build && ./divyield.exe chart `ls work/stocks`


