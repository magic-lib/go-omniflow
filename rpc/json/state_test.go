package json_test

import (
	"fmt"
	jsonData "github.com/magic-lib/go-omniflow/rpc/json"
	"github.com/magic-lib/go-plat-utils/conn"
	"testing"
)

func TestTCPJsonRPC(t *testing.T) {
	server, err := jsonData.NewRpcService("tcp", []*jsonData.ServiceRegistration{
		{
			Handler: new(jsonData.Arith),
		},
	})
	if err != nil {
		panic(err)
	}
	server.WithServerPort(1234)

	err = server.StartServer()
	if err != nil {
		panic(err)
	}

	client, err := jsonData.NewRpcClient("tcp", &conn.Connect{
		Host: "127.0.0.1",
		Port: "1234",
	})
	if err != nil {
		panic(err)
	}

	var reply int
	err = client.Submit("Arith.Add", &jsonData.Args{A: 10, B: 20}, &reply)
	if err != nil {
		panic(err)
	}
	fmt.Printf("tcp: 10 + 20 = %d\n", reply) // 输出：10 + 20 = 30

	TestHTTPJsonRPC(t)
}
func TestTCPMsgRPC(t *testing.T) {
	server, err := jsonData.NewRpcService("tcp", []*jsonData.ServiceRegistration{
		{
			Handler: new(jsonData.Arith),
		},
	})
	if err != nil {
		panic(err)
	}
	server.WithServerPort(1234)
	server.WithTcpRPCCodec("msgpack")

	err = server.StartServer()
	if err != nil {
		panic(err)
	}

	client, err := jsonData.NewRpcClient("tcp", &conn.Connect{
		Host: "127.0.0.1",
		Port: "1234",
	})
	if err != nil {
		panic(err)
	}
	client.WithTcpRPCCodec("msgpack")

	var reply int
	err = client.Submit("Arith.Add", &jsonData.Args{A: 10, B: 20}, &reply)
	if err != nil {
		panic(err)
	}
	fmt.Printf("msgpack tcp: 10 + 20 = %d\n", reply) // 输出：10 + 20 = 30
}
func TestHTTPJsonRPC(t *testing.T) {
	server, err := jsonData.NewRpcService("http", []*jsonData.ServiceRegistration{
		{
			Handler: new(jsonData.Arith),
		},
	})
	if err != nil {
		panic(err)
	}
	server.WithServerPort(1235)
	server.WithHttpRPCPath("/rpc")

	err = server.StartServer()
	if err != nil {
		panic(err)
	}

	client, err := jsonData.NewRpcClient("http", &conn.Connect{
		Host: "127.0.0.1",
		Port: "1235",
	})
	if err != nil {
		panic(err)
	}
	client.WithHttpRPCPath("/rpc")

	var reply jsonData.Result
	err = client.Submit("Arith.HttpAdd", &jsonData.Args{A: 10, B: 20}, &reply)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("http: 10 + 20 = %d\n", reply.Value) // 输出：10 + 20 = 30
}
