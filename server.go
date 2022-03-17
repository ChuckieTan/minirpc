package minirpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"minirpc/codec"
	"net"
	"reflect"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

const MagicNumber = 0x065279

type Option struct {
	MagicNumber int
	CodecType   codec.Type
}

var DefaultCodecType = codec.GobType

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

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

func (server *Server) Accept(linstener net.Listener) {
	for {
		conn, err := linstener.Accept()
		if err != nil {
			logrus.Errorf("minirpc.Server.Accept: %v", err)
			return
		}
		go server.HandleConn(conn)
	}
}

// HandleConn 处理单个连接，并阻塞程序运行直到连接关闭
func (server *Server) HandleConn(conn io.ReadWriteCloser) {
	defer conn.Close()
	// 首先读取报文的头部
	option, err := server.readOption(conn)
	if err != nil {
		logrus.Error(err)
		return
	}

	// 然后获取对应的编码器
	codecFunc, ok := codec.NewCodecFuncMap[option.CodecType]
	if !ok {
		logrus.Errorf("minirpc.Server.HandleConn: codec type error")
		return
	}
	codec := codecFunc(conn)
	server.handleCodec(codec)
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
func (server *Server) handleCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			logrus.Info("minirpc.Server.handleCodec: read request error:", err)
			req.header.Error = err.Error()
			go server.sendResponse(cc, req.header, req, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg)
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

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	err := req.svc.call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.header.Error = err.Error()
	}
	server.sendResponse(cc, req.header, req.replyv.Interface(), sending)
}

func Accept(linstener net.Listener) {
	DefaultServer.Accept(linstener)
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}
