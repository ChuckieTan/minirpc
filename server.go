package minirpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"minirpc/codec"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const MagicNumber = 0x065279

// 服务器和客户端的头信息
type Option struct {
	MagicNumber int
	CodecType   codec.Type
	// 连接超时时间，0 表示无限制
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultCodecType = codec.GobType

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 3,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// 注册一个结构体的所有方法
func (server *Server) Register(rcvr interface{}) error {
	svc := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(svc.name, svc); dup {
		return errors.New("rpc: service already defined: " + svc.name)
	}
	return nil
}

// 根据方法名获取对应的方法和 service
// service 表示一个被注册的类型和他的方法
// mtype 表示要被调用的方法
func (server *Server) findService(serviceMethod string) (*service, *methodType, error) {
	strs := strings.Split(serviceMethod, ".")
	if len(strs) != 2 {
		return nil, nil, errors.New("rpc: service/method request ill-formed: " + serviceMethod)
	}
	serviceName, methodName := strs[0], strs[1]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		return nil, nil, errors.New("rpc: can't find service " + serviceName)
	}
	svc := svci.(*service)
	mtype, ok := svc.method[methodName]
	if !ok {
		return nil, nil, errors.New("rpc: can't find method " + methodName)
	}
	return svc, mtype, nil
}

// 接收一个连接并处理请求
func (server *Server) Accept(linstener net.Listener) {
	for {
		conn, err := linstener.Accept()
		if err != nil {
			logrus.Errorf("minirpc.Server.Accept: %v", err)
			return
		}
		// logrus.Info("connection from: ", conn.RemoteAddr())
		go server.HandleConn(conn)
	}
}

// HandleConn 处理单个连接，并阻塞程序运行直到连接关闭
func (server *Server) HandleConn(conn io.ReadWriteCloser) {
	defer conn.Close()
	// 读取报文的头部
	option, err := server.readOption(conn)
	if err != nil {
		logrus.Error(err)
		return
	}

	// 获取对应的编码器
	codecFunc, ok := codec.NewCodecFuncMap[option.CodecType]
	if !ok {
		logrus.Errorf("minirpc.Server.HandleConn: codec type error")
		return
	}
	codec := codecFunc(conn)
	// 两次握手，解决 TCP 粘包问题
	if err := json.NewEncoder(conn).Encode(option); err != nil {
		logrus.Error("minirpc.Server.HandleConn: option error: ", err)
		return
	}
	server.handleCodec(codec, option)
}

// 获取报文头部
func (server *Server) readOption(conn io.ReadWriteCloser) (*Option, error) {
	var option Option
	if err := json.NewDecoder(conn).Decode(&option); err != nil {
		err := fmt.Errorf("minirpc.readOption: %v", err)
		return nil, err
	}
	if option.MagicNumber != MagicNumber {
		err := errors.New("minirpc.readOption: magic number error: not a minirpc connection")
		return nil, err
	}

	return &option, nil
}

type request struct {
	// 请求头
	header *codec.Header
	// 参数和返回值
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

var invalidRequest = struct{}{}

// 通过编码器处理后续请求，每个请求并发执行
func (server *Server) handleCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			req.header.Error = err.Error()
			go server.sendResponse(cc, req.header, req, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// 通过编码器发送一个 response
func (server *Server) sendResponse(
	cc codec.Codec, header *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(header, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 读取 request 的 header 部分
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var header codec.Header
	if err := cc.ReadHeader(&header); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			logrus.Error("read header error: ", err)
		}
		return nil, err
	}
	return &header, nil
}

// 读取一个 request，包括 header 和 body
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	header, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{
		header: header,
	}
	req.svc, req.mtype, err = server.findService(header.ServiceMethod)
	if err != nil {
		logrus.Error("minirpc.Server.readRequest: ", err)
		return nil, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReply()

	argvi := req.argv.Interface()
	if req.argv.Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err := cc.ReadBody(argvi); err != nil {
		logrus.Error("read body error: ", err)
		return nil, err
	}
	return req, nil
}

// 处理请求，并发送回应
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.header.Error = err.Error()
			server.sendResponse(cc, req.header, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.header, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	case <-called:
		// 如果调用成功，则等待发送完成
		<-sent
	case <-time.After(timeout):
		// 如果调用超时，则发送超时错误，并关闭连接
		logrus.Error("minirpc.Server.handleRequest: call timeout")
		server.sendResponse(cc, req.header, invalidRequest, sending)
		_ = cc.Close()
	}
}

// 使用默认的服务器监听
func Accept(linstener net.Listener) {
	DefaultServer.Accept(linstener)
}

// 注册一个结构体的所有方法到默认的服务器
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

const (
	connected        = "200 Connected to minirpc"
	defaultRPCPath   = "/_minirpc_"
	defaultDebugPath = "/_minirpc_debug_"
)

// 实现了 http.Handler 接口，以进行 http 之上的 RPC 通信
// 使用 HTTP 中的 Connect 方法进行连接
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		logrus.Error("minirpc.ServeHTTP: hijack error: ", err)
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.HandleConn(conn)
}

// 注册相应的地址为 http path
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, DebugHTTP{server})
	logrus.Info("minirpc.Server.HandleHTTP: http server started")
	logrus.Info("minirpc.Server.HandleHTTP: http server listen on:", defaultRPCPath)
	logrus.Info("minirpc.Server.HandleHTTP: debug server listen on:", defaultDebugPath)
}

// 使用默认的服务器处理 HTTP 请求
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
