
go build cmd/divyield/main.go

for i in `cat urls.csv`; do
    ./main.exe bs "$i"
done

