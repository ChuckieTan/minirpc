package xclient

import (
	"context"
	"minirpc"
	"reflect"
	"sync"

	"github.com/sirupsen/logrus"
)

type XClient struct {
	d    Discovery
	mode SelectMode
	opt  *minirpc.Option
	// 已经建立好对应服务器的连接的客户端，可以复用
	clients map[string]*minirpc.Client
	mu      sync.Mutex
}

func NewXClient(d Discovery, mode SelectMode, opt *minirpc.Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*minirpc.Client),
	}
}

// 关闭所有的连接
func (c *XClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, client := range c.clients {
		client.Close()
	}
	c.clients = make(map[string]*minirpc.Client)
	return nil
}

// 获取对应地址的客户端
func (c *XClient) dial(rpcAddr string) (*minirpc.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, ok := c.clients[rpcAddr]
	// 如果客户端已不再可用，则关闭并删除
	if ok && !client.Avaliable() {
		client.Close()
		logrus.Warn("client is not avaliable, close it")
		delete(c.clients, rpcAddr)
		client = nil
	}
	// 如果客户端不存在，则创建一个新的
	if client == nil {
		// 建立新的连接
		client, err := minirpc.XDial(rpcAddr, c.opt)
		if err != nil {
			return nil, err
		}
		c.clients[rpcAddr] = client
	}
	return c.clients[rpcAddr], nil
}

// 发起对应地址的调用
func (c *XClient) call(
	ctx context.Context, rpcAddr string, serviceMethod string, args, reply interface{}) error {
	client, err := c.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// 选择一个服务器发起调用
func (c *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := c.d.Get(c.mode)
	if err != nil {
		return err
	}
	return c.call(ctx, rpcAddr, serviceMethod, args, reply)
}

// Broadcast 将调用广播到所有的服务器，并给赋值给 reply 其中一个值
// 如果有一个服务器返回错误，则返回错误
func (c *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := c.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	err = nil
	var assignLock sync.Mutex
	// 判断是否已经赋值的信号
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			// 先复制一份 reply，避免多线程写出错
			replyClone := reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			// call 的返回值 err 只有第一次出错复制到要返回的返回值中
			e := c.call(ctx, rpcAddr, serviceMethod, args, replyClone)
			assignLock.Lock()
			if err == nil && e != nil {
				err = e
				cancel()
			}
			if e == nil && !replyDone {
				replyDone = true
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(replyClone).Elem())
			}
			assignLock.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return err
}
