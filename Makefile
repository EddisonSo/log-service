.PHONY: proto build clean

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/logging/logging.proto

build: proto
	go build -o log-service .

clean:
	rm -f log-service
