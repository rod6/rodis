module github.com/rod6/rodis

go 1.14

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/garyburd/redigo v1.6.0
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/libgo/logx v1.0.5
	github.com/pborman/uuid v1.2.0
	github.com/syndtr/goleveldb v1.0.0
	golang.org/x/text v0.3.2 // indirect
)

replace github.com/rod6/rodis => ./
