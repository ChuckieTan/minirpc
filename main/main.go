package main

import (
	"context"
	"minirpc"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"minirpc/main/logfmt"

	"github.com/sirupsen/logrus"
)

func init() {
	//设置output,默认为stderr,可以为任何io.Writer，比如文件*os.File
	logrus.SetOutput(os.Stdout)
	//设置最低loglevel
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logfmt.MyFormatter{})
}

type Foo struct{}

type Args struct {
	Num1, Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func startServer(addr chan string) {
	var foo Foo
	if err := minirpc.Register(&foo); err != nil {
		logrus.Fatalf("register error: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:9999")
	if err != nil {
		logrus.Fatalf("listen error: %v", err)
	}
	minirpc.HandleHTTP()
	logrus.Infof("listen on %s", listener.Addr().String())
	addr <- listener.Addr().String()
	http.Serve(listener, nil)
}

func call(addr chan string) {
	client, err := minirpc.XDial("http://" + <-addr)
	if err != nil {
		logrus.Fatalf("dial error: %v", err)
	}
	defer func() {
		err = client.Close()
		if err != nil {
			logrus.Error("close client error: %v", err)
		}
	}()

	var wg sync.WaitGroup
	// 当服务器没准备好时客户端就发送消息，服务端接收到的消息会不完整
	// 所以要等服务端准备好
	time.Sleep(time.Second)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var reply int
			args := Args{i, i * i}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := client.Call(ctx, "Foo.Sum", args, &reply)
			if err != nil {
				logrus.Errorf("call error: %v", err)
			}
			logrus.Infof("args: %v, reply: %v", args, reply)
		}(i)
	}
	wg.Wait()
}

func main() {
	// buf := bytes.NewBuffer([]byte{})
	// a := codec.NewGobCodec(buf)
	addr := make(chan string)
	go call(addr)
	startServer(addr)
}
