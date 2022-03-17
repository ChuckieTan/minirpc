package minirpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"minirpc/codec"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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

func (client *Client) Avaliable() bool {
	client.lock.Lock()
	defer client.lock.Unlock()
	return client.avaliable()
}

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
		if opt.MagicNumber != MagicNumber {
			return nil, errors.New("invalid magic number")
		}
		if opt.CodecType != codec.JsonType && opt.CodecType != codec.GobType {
			return nil, errors.New("invalid codec type")
		}
		if opt.CodecType == "" {
			opt.CodecType = DefaultOption.CodecType
		}
		return opt, nil
	}
}

type NewClientFunc func(conn net.Conn, opt *Option) (*Client, error)

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

type clientResult struct {
	client *Client
	err    error
}

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

// 异步接口，直接返回 call 实例
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 1)
	} else if len(done) == 0 {
		logrus.Error("done channel is not buffered")
		close(done)
		return nil
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

// 同步接口，一直等待返回结果，在 context 超时时会返回错误
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
