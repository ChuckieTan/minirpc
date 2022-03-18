package main

import (
	"context"
	"minirpc"
	"net"
	"os"
	"sync"
	"time"

	"minirpc/main/logfmt"
	"minirpc/xclient"

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

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Duration(args.Num1) * time.Second)
	*reply = args.Num1 + args.Num2
	return nil
}

func startServer(addr chan string) {
	var foo Foo
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	server := minirpc.NewServer()
	server.Register(foo)
	logrus.Infof("listen on %s", listener.Addr().String())
	addr <- listener.Addr().String()
	server.Accept(listener)
	// http.Serve(listener, nil)
}

// 在普通 call 或 broadcast 成功或失败后打印日志
func foo(ctx context.Context, xc *xclient.XClient, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = xc.Broadcast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		logrus.Errorf("%s %s, error: %v", typ, serviceMethod, err)
	} else {
		logrus.Infof("%s %s, reply: %d", typ, serviceMethod, reply)
	}
}

func call(addr1, addr2 string) {
	d := xclient.NewMultiDiscovery([]string{"tcp://" + addr1, "tcp://" + addr2})
	xc := xclient.NewXClient(d, xclient.SelectMode_Random, nil)
	defer xc.Close()
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(context.Background(), xc, "call", "Foo.Sum", &Args{i, i * i})
		}(i)
	}
	wg.Wait()
}

func broadcast(addr1, addr2 string) {
	d := xclient.NewMultiDiscovery([]string{"tcp://" + addr1, "tcp://" + addr2})
	xc := xclient.NewXClient(d, xclient.SelectMode_Random, nil)
	defer xc.Close()
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(context.Background(), xc, "broadcast", "Foo.Sum", &Args{i, i * i})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()
			foo(ctx, xc, "broadcast", "Foo.Sleep", &Args{i, i * i})
		}(i)
	}
	wg.Wait()
}

func main() {
	ch1 := make(chan string)
	ch2 := make(chan string)
	go startServer(ch1)
	go startServer(ch2)
	addr1 := <-ch1
	addr2 := <-ch2
	time.Sleep(time.Second)
	call(addr1, addr2)
	broadcast(addr1, addr2)
}
