package json

import "net/http"

// 定义参数和返回值结构
type Args struct {
	A, B int
}

type Arith int // 服务结构体

// 远程方法：加法
func (t *Arith) Add(args *Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

// Result 加法响应结果
type Result struct {
	Value int `json:"value"`
}

// HttpAdd 加法方法（RPC 可调用）
func (a *Arith) HttpAdd(r *http.Request, args *Args, result *Result) error {
	result.Value = args.A + args.B
	return nil
}
