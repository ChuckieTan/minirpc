package minirpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"minirpc/codec"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call 表示一个 RPC 调用
type Call struct {
	// 请求的序号，用来区分不同的请求
	Seq uint64
	// 服务名和方法名，格式为 "Service.Method"
	ServiceMethod string
	// 要调用的方法的参数
	Args interface{}
	// 方法的返回值
	Reply interface{}
	// 返回的错误信息
	Err error
	// 方法调用结束时的信号
	Done chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

// Client 表示一个 RPC 客户端
// 一个客户端可以同时调用多个方法
type Client struct {
	// 编码器
	cc codec.Codec
	// CS数据交换的头信息
	option Option
	// 发送数据的互斥锁
	sending sync.Mutex
	// Client 操作的互斥锁
	lock sync.Mutex
	// 当前发送的序号
	seq uint64
	// 正在等待的调用
	pending map[uint64]*Call
	// 客户端正常退出
	closed bool
	// 客户端非正常退出
	shutdown bool
}

var _ io.Closer = (*Client)(nil)

var ErrClientShutdown = errors.New("client is shutdown")

func (client *Client) Close() error {
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.closed || client.shutdown {
		return ErrClientShutdown
	}
	client.closed = true
	return client.cc.Close()
}

// 判断客户端是否可用
func (client *Client) Avaliable() bool {
	client.lock.Lock()
	defer client.lock.Unlock()
	return client.avaliable()
}

// 内部使用的 avvaliable 方法，无锁
func (client *Client) avaliable() bool {
	return !client.shutdown && !client.closed
}

// 将 call 加入到 client 的 pending 中，并更新 seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.lock.Lock()
	defer client.lock.Unlock()
	if !client.avaliable() {
		return 0, ErrClientShutdown
	}
	seq := client.seq
	call.Seq = seq
	client.pending[seq] = call
	client.seq++
	return seq, nil
}

// 移除 seq 对应的调用
func (client *Client) removeCall(seq uint64) *Call {
	client.lock.Lock()
	defer client.lock.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 当服务端或客户端发生错误时调用
// 异常退出 client，终止所有调用并通知其对应的 error
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.lock.Lock()
	defer client.lock.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Err = err
		call.done()
	}
}

// 循环接收服务端发送的数据，分为 header 和 body 两部分
func (client *Client) recieve() {
	var err error
	// 只有在出错的时候才退出循环
	for err == nil {
		var header codec.Header
		if err = client.cc.ReadHeader(&header); err != nil {
			// if err != io.EOF && err != io.ErrUnexpectedEOF {
			// 	logrus.Errorf("read header error: %v", err)
			// }
			return
		}
		call := client.removeCall(header.Seq)
		if call == nil {
			err = client.cc.ReadBody(nil)
			continue
		} else if header.Error != "" {
			call.Err = errors.New(header.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		} else {
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Err = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	client.terminateCalls(errors.New("recieve error"))
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	newCodecFunc := codec.NewCodecFuncMap[opt.CodecType]
	if newCodecFunc == nil {
		return nil, fmt.Errorf("unsupported codec type: %v", opt.CodecType)
	}
	cc := newCodecFunc(conn)
	// 发送 option
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		return nil, err
	}

	client := &Client{
		cc:       cc,
		option:   *opt,
		pending:  make(map[uint64]*Call),
		closed:   false,
		shutdown: false,
		seq:      1,
		sending:  sync.Mutex{},
		lock:     sync.Mutex{},
	}
	go client.recieve()
	return client, nil
}

func parseOption(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	} else if len(opts) != 1 {
		return nil, errors.New("only one option is supported")
	} else {
		opt := opts[0]
		opt.MagicNumber = MagicNumber
		if opt.CodecType == "" {
			opt.CodecType = DefaultOption.CodecType
		}
		return opt, nil
	}
}

type NewClientFunc func(conn net.Conn, opt *Option) (*Client, error)

// 通过 TCP 方式连接服务端，具有超时功能
func DialTCP(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// 连接到指定地址的服务器，超时返回错误
func dialTimeout(f NewClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOption(opts...)
	if err != nil {
		return nil, err
	}
	// 在 dial 的时候会阻塞，直到连接成功或超时
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	// 异步创建客户端需要传出客户端和 error
	// 所以需要新建一个结构体来放入 chan 里面
	type clientResult struct {
		client *Client
		err    error
	}
	// 开启一个新协程创建客户端
	ch := make(chan clientResult, 1)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client, err}
	}()

	// 如果超时时间为 0，则一直等待
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}

	// 在主线程阻塞等待超时时间
	// 如果超时时间内还没接收到 clientResult，则认为超时
	select {
	case result := <-ch:
		return result.client, result.err
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout expect within %v", opt.ConnectTimeout)
	}
}

// 发送数据
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Err = err
		call.done()
		return
	}

	// 构造 header
	header := codec.Header{
		ServiceMethod: call.ServiceMethod,
		Seq:           seq,
		Error:         "",
	}
	// 发送 header 和 参数
	if err := client.cc.Write(&header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Err = err
			call.done()
		}
	}
}

// 对服务器发起调用
// 异步接口，直接返回 call 实例
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 1)
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Err:           nil,
		Done:          done,
	}
	go client.send(call)
	return call
}

// 对服务器发起调用，并等待返回
// 在 context 超时时会返回错误
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		// 如果超时，则取消发送
		client.removeCall(call.Seq)
		return fmt.Errorf("rpc client: call timeout expect within %v", ctx.Err())
	case call := <-call.Done:
		return call.Err
	}
}

// 带有超时功能的调用
func (client *Client) CallTimeout(serviceMethod string, args, reply interface{}, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.Call(ctx, serviceMethod, args, reply)
}

// 新建一个支持 HTTP 协议的客户端
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// 使用 connect 方式连接到服务器，之后切换到 RPC 模式
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	// 客户端返回的状态不对
	if err == nil {
		err = errors.New("unexpected connect status: " + resp.Status)
	}
	return nil, err
}

// 通过 HTTP 协议连接到服务器
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, "tcp", address, opts...)
}

// XDial 方法用于自定义连接方式
// rpcAddress 的格式类似于 tcp://127.0.0.1:7001, http://127.0.0.1:7001 等
func XDial(rpcAddress string, opts ...*Option) (*Client, error) {
	// 分割字符串，得到网络类型和地址
	network, address, err := splitRPCAddress(rpcAddress)
	if err != nil {
		return nil, err
	}

	switch network {
	case "tcp", "tcp4", "tcp6":
		return DialTCP(network, address, opts...)
	case "unix":
		return DialTCP(network, address, opts...)
	case "http", "https":
		return DialHTTP("tcp", address, opts...)
	default:
		return nil, fmt.Errorf("rpc client: unknown network %q", network)
	}
}

// 分割 RPC 地址，得到网络类型和地址
func splitRPCAddress(rpcAddress string) (string, string, error) {
	strs := strings.Split(rpcAddress, "://")
	if len(strs) != 2 {
		return "", "", fmt.Errorf("invalid rpc address: %s", rpcAddress)
	}
	return strs[0], strs[1], nil
}
