PROTOC_GEN_GO := /Volumes/MacintoshData/WorkSpace/goland-framework/bin/protoc-gen-go
PROTOC_GEN_CONNECT_GO := /Volumes/MacintoshData/WorkSpace/goland-framework/bin/protoc-gen-connect-go


build-connect-rpc-gen:
	#go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	#go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	protoc --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative \
 	./pb/greet.proto