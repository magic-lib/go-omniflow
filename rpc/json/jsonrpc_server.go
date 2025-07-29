//go:build !js && !wasip1

package json

import (
	"fmt"
	"github.com/google/uuid"
	httprpc "github.com/gorilla/rpc/v2"
	httpjson "github.com/gorilla/rpc/v2/json"
	"github.com/hashicorp/go-multierror"
	"github.com/magic-lib/go-plat-utils/conn"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"github.com/samber/lo"
	"github.com/ugorji/go/codec"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type RpcService struct {
	network           string
	httpServer        *httprpc.Server
	httpServerHandler http.Handler
	tcpClientCodec    string
	tcpServer         *rpc.Server
	listener          net.Listener
	pathAddr          string
	serverConn        *conn.Connect
}

// ServiceRegistration 调整后的结构体：用于封装服务注册信息
// Name: 服务注册时的名称（如"net.rpc.Arith"）
// Handler: 服务实例（实现具体RPC方法的对象）
type ServiceRegistration struct {
	Name    string
	Handler any
}

func NewRpcService(network string, services []*ServiceRegistration) (*RpcService, error) {
	if len(services) == 0 {
		return nil, fmt.Errorf("services is empty")
	}

	if network == "tcp" {
		newServer, err := newTcpServer(services)
		if err != nil {
			return nil, err
		}
		newServer.network = network
		return newServer, nil
	} else if network == "http" {
		newServer, err := newHttpServer(services)
		if err != nil {
			return nil, err
		}
		newServer.network = network
		return newServer, nil
	}
	return nil, fmt.Errorf("network is not tcp or http")
}

func (s *RpcService) WithServerPort(port int) *RpcService {
	if port > 0 {
		if s.serverConn == nil {
			s.serverConn = new(conn.Connect)
		}
		s.serverConn.Port = conv.String(port)
	}
	return s
}
func (s *RpcService) WithServerListener(l net.Listener) *RpcService {
	if l != nil {
		s.listener = l
	}
	return s
}

func (s *RpcService) WithTcpRPCCodec(codec string) *RpcService {
	if codec == "" {
		return s
	}
	s.tcpClientCodec = codec
	return s
}
func (s *RpcService) WithHttpRPCPath(path string) *RpcService {
	if path == "" {
		return s
	}
	s.pathAddr = path
	return s
}

func newTcpServer(services []*ServiceRegistration) (*RpcService, error) {
	newServer := rpc.NewServer()
	var retErr error
	lo.ForEach(services, func(service *ServiceRegistration, index int) {
		if service.Handler == nil {
			return
		}
		if service.Name == "" {
			err := newServer.Register(service.Handler)
			if err != nil {
				retErr = multierror.Append(retErr, err)
			}
			return
		}
		err := newServer.RegisterName(service.Name, service.Handler)
		if err != nil {
			retErr = multierror.Append(retErr, err)
		}
	})

	if retErr != nil {
		return nil, retErr
	}
	rpcInfo := new(RpcService)
	rpcInfo.tcpServer = newServer
	return rpcInfo, nil
}
func newHttpServer(services []*ServiceRegistration) (*RpcService, error) {
	newServer := httprpc.NewServer()
	newServer.RegisterCodec(httpjson.NewCodec(), "application/json")
	var retErr error
	lo.ForEach(services, func(service *ServiceRegistration, index int) {
		if service.Handler == nil {
			return
		}
		err := newServer.RegisterService(service.Handler, service.Name)
		if err != nil {
			retErr = multierror.Append(retErr, err)
		}
	})

	if retErr != nil {
		return nil, retErr
	}
	rpcInfo := new(RpcService)
	rpcInfo.httpServer = newServer
	rpcInfo.httpServerHandler = newServer
	return rpcInfo, nil
}

func defaultListenTCP(port int) (net.Listener, string, error) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	l, err := net.Listen("tcp", addr) // any available address
	if err != nil {
		return nil, "", err
	}
	return l, l.Addr().String(), nil
}

func (s *RpcService) StartServer() error {
	if s.listener == nil {
		port := 0
		if s.serverConn != nil {
			port, _ = conv.Int(s.serverConn.Port)
		}
		l, newServerAddr, err := defaultListenTCP(port)
		if err != nil {
			return err
		}
		log.Println("listener addr:", newServerAddr)
		s.listener = l
	}

	if s.tcpServer != nil {
		goroutines.GoAsync(func(params ...any) {
			defer func() {
				_ = s.listener.Close()
			}()

			for {
				tcpConn, err := s.listener.Accept()
				if err != nil {
					fmt.Print("rpc.Serve: accept:", err.Error())
					continue
				}
				if s.tcpClientCodec == "msgpack" {
					go s.tcpServer.ServeCodec(codec.MsgpackSpecRpc.ServerCodec(tcpConn, &codec.MsgpackHandle{}))
				} else {
					go s.tcpServer.ServeCodec(jsonrpc.NewServerCodec(tcpConn))
				}
			}
		})
		return nil
	}
	if s.httpServer != nil && s.httpServerHandler != nil {
		goroutines.GoAsync(func(params ...any) {
			defer func() {
				_ = s.listener.Close()
			}()
			if s.pathAddr == "" {
				s.pathAddr = fmt.Sprintf("/%s", uuid.NewString())
			}
			fmt.Println("httprpc server path:", s.pathAddr)
			http.Handle(s.pathAddr, s.httpServer)
			_ = http.Serve(s.listener, s.httpServerHandler)
		})
		return nil
	}
	return fmt.Errorf("not support")
}
