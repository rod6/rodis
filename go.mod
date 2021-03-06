module github.com/rod6/rodis

go 1.14

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/aws/aws-sdk-go v1.30.14
	github.com/garyburd/redigo v1.6.0
	github.com/go-delve/delve v1.4.0
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/libgo/logx v1.0.5
	github.com/pborman/uuid v1.2.0
	github.com/syndtr/goleveldb v1.0.0
	golang.org/x/text v0.3.2 // indirect
	google.golang.org/appengine v1.6.6
	honnef.co/go/tools v0.0.1-2020.1.3
)

replace github.com/rod6/rodis => ./
