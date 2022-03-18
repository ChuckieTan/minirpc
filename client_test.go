package minirpc

import (
	"context"
	"log"
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestClient_dialTimeout(t *testing.T) {
	t.Parallel()
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()
	f := func(conn net.Conn, opt *Option) (*Client, error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", listener.Addr().String(), &Option{
			ConnectTimeout: time.Second,
		})
		if err == nil {
			t.Fatal("should timeout")
		}
	})
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", listener.Addr().String(), &Option{
			ConnectTimeout: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

type Bar struct{}

func (bar Bar) Timeout(args int, reply *int) error {
	time.Sleep(time.Second * 2)
	*reply = args
	return nil
}

func startServer(addr chan string) {
	var b Bar
	Register(b)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr <- listener.Addr().String()
	Accept(listener)
}

func TestClient_Call(t *testing.T) {
	t.Parallel()
	addrCh := make(chan string)
	go startServer(addrCh)
	addr := <-addrCh
	log.Println("server listen on:", addr)
	time.Sleep(time.Second)
	t.Run("client timeout", func(t *testing.T) {
		client, err := DialTCP("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var reply int
		err = client.Call(ctx, "Bar.Timeout", 1, &reply)
		if err == nil {
			t.Fatal("should timeout")
		}
	})
	t.Run("server timeout", func(t *testing.T) {
		client, err := DialTCP("tcp", addr, &Option{
			HandleTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var reply int
		err = client.Call(ctx, "Bar.Timeout", 1, &reply)
		if err == nil {
			t.Fatal("should timeout")
		}
	})
}

func TestClient_XDial(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip windows")
	}
	if runtime.GOOS == "linux" {
		ch := make(chan string)
		addr := "/tmp/minirpc.sock"
		go func() {
			_ = os.Remove(addr)
			listener, err := net.Listen("unix", addr)
			if err != nil {
				panic(err)
			}
			ch <- listener.Addr().String()
			Accept(listener)
		}()
		<-ch
		_, err := XDial("unix://" + addr)
		if err != nil {
			t.Fatal(err)
		}
	}
}
