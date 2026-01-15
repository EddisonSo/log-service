module github.com/eddisonso/log-service

go 1.24.0

toolchain go1.24.11

require (
	eddisonso.com/go-gfs v0.0.0
	github.com/gorilla/websocket v1.5.3
	google.golang.org/grpc v1.73.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 // indirect
)

replace eddisonso.com/go-gfs => ../go-gfs


