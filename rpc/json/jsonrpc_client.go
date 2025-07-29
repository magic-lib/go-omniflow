//go:build !js && !wasip1

package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/conn"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/ugorji/go/codec"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"reflect"
)

type RpcClient struct {
	network        string
	serverAddr     string
	pathAddr       string
	tcpClientCodec string
	serverConn     *conn.Connect
}

func NewRpcClient(network string, serverConn *conn.Connect) (*RpcClient, error) {
	return &RpcClient{
		network:    network,
		serverConn: serverConn,
	}, nil
}

func (s *RpcClient) WithHttpRPCPath(path string) *RpcClient {
	if path == "" {
		return s
	}
	s.pathAddr = path
	return s
}
func (s *RpcClient) WithTcpRPCCodec(codec string) *RpcClient {
	if codec == "" {
		return s
	}
	s.tcpClientCodec = codec
	return s
}
func (s *RpcClient) WithServerAddr(addr string) *RpcClient {
	if addr == "" {
		return s
	}
	s.serverAddr = addr
	return s
}

func (s *RpcClient) execTcpClient(serviceMethod string, args any, reply any) error {
	// 连接服务端
	serverAddr := ""
	if s.serverConn != nil {
		if s.serverConn.Host != "" {
			serverAddr = net.JoinHostPort(s.serverConn.Host, s.serverConn.Port)
		}
	}
	if serverAddr == "" {
		serverAddr = s.serverAddr
	}

	if serverAddr == "" {
		return fmt.Errorf("serverAddr is empty")
	}

	var client *rpc.Client
	var err error

	if s.tcpClientCodec == "msgpack" {
		connTcp, err := net.Dial("tcp", serverAddr)
		if err != nil {
			return err
		}
		clientCodec := codec.MsgpackSpecRpc.ClientCodec(connTcp, &codec.MsgpackHandle{})
		client = rpc.NewClientWithCodec(clientCodec)
	} else {
		client, err = jsonrpc.Dial("tcp", serverAddr)
	}

	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	err = client.Call(serviceMethod, args, reply)
	if err != nil {
		return err
	}
	return nil
}

type jSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   any             `json:"error"`
	Id      string          `json:"id"`
}

func (s *RpcClient) execHttpClient(serviceMethod string, args any, reply any) error {
	reqJSON := conv.String(map[string]any{
		"jsonrpc": "2.0",
		"method":  serviceMethod,
		"params":  []any{args},
		"id":      uuid.NewString(),
	})

	serverAddr := ""
	if s.serverConn != nil {
		if s.serverConn.Host != "" {
			serverAddr = net.JoinHostPort(s.serverConn.Host, s.serverConn.Port)
		}
	}
	if serverAddr == "" {
		serverAddr = s.serverAddr
	}
	if serverAddr == "" || s.pathAddr == "" {
		return fmt.Errorf("serverAddr or pathAddr is empty")
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s%s", serverAddr, s.pathAddr),
		"application/json",
		bytes.NewBufferString(reqJSON),
	)

	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// 4. 解析响应
	var rpcResp jSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}

	// 5. 处理结果
	if rpcResp.Error != nil {
		return fmt.Errorf("调用失败: %v\n", rpcResp.Error)
	}

	err = conv.Unmarshal(rpcResp.Result, reply)
	if err != nil {
		return err
	}
	return nil
}

func (s *RpcClient) Submit(serviceMethod string, args any, reply any) error {
	if s.network == "tcp" {
		//args 和 reply 都是指针类型
		if !cond.IsPointer(args) {
			return fmt.Errorf("args is not pointer")
		}
		if !cond.IsPointer(reply) {
			return fmt.Errorf("reply is not pointer")
		}
		return s.execTcpClient(serviceMethod, args, reply)
	}

	if s.network == "http" {
		if !cond.IsPointer(args) {
			return fmt.Errorf("args is not struct pointer")
		}
		if !cond.IsPointer(reply) {
			return fmt.Errorf("reply is not struct pointer")
		}

		if reflect.Indirect(reflect.ValueOf(args)).Type().Kind() != reflect.Struct {
			return fmt.Errorf("args is not struct pointer")
		}
		if reflect.Indirect(reflect.ValueOf(reply)).Type().Kind() != reflect.Struct {
			return fmt.Errorf("reply is not struct pointer")
		}

		return s.execHttpClient(serviceMethod, args, reply)
	}

	return fmt.Errorf("not support")
}
