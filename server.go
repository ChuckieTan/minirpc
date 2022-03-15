package minirpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"minirpc/codec"
	"net"
	"reflect"
	"sync"

	"github.com/sirupsen/logrus"
)

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

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
func (server *Server) readOption(conn io.ReadWriteCloser) (*codec.Option, error) {
	var option codec.Option
	if err := json.NewDecoder(conn).Decode(&option); err != nil {
		err := fmt.Errorf("minirpc.readOption: %v", err)
		return nil, err
	}
	if option.MagicNumber != codec.MagicNumber {
		err := errors.New("minirpc.readOption: magic number error: not a minirpc connection")
		return nil, err
	}

	return &option, nil
}

// 通过编码器处理后续请求，每个请求并发执行
func (server *Server) handleCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	respChan := make(chan *codec.Response, 16)
	// 发送 channel 里面的 response
	go func() {
		for resp := range respChan {
			sending.Lock()
			server.sendResponse(cc, resp)
			sending.Unlock()
			wg.Done()
		}
	}()
	for {
		// 首先读取 request
		request, err := server.readRequest(cc)
		if err != nil {
			logrus.Error(err)
			close(respChan)
			break
		}
		wg.Add(1)
		go server.handleRequest(request, respChan)
	}
	wg.Wait()
}

func (server *Server) sendResponse(cc codec.Codec, resp *codec.Response) {
	if err := cc.Write(resp); err != nil {
		logrus.Errorf("minirpc.sendResponse: %v", err)
		return
	}
}

func (server *Server) readRequest(cc codec.Codec) (*codec.Request, error) {
	var req codec.Request
	if err := cc.Read(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

func (server *Server) handleRequest(req *codec.Request, respChan chan *codec.Response) {
	response := new(codec.Response)
	response.Seq = req.Seq
	// 先只输出参数
	logrus.Info(req.Args)
	// 假设返回值是 string
	response.Reply = reflect.ValueOf(fmt.Sprintf("minipc resp %d", req.Seq))
	respChan <- response
}

func Accept(linstener net.Listener) {
	DefaultServer.Accept(linstener)
}
