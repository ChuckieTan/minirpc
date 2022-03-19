package registry

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type MiniRegistry struct {
	// 超时时间
	timeout time.Duration
	// 服务器列表
	servers map[string]*ServerItem
	mu      sync.Mutex
}

// 一个 server item 表示一个服务器
type ServerItem struct {
	// 服务器地址
	Addr string
	// 服务器上一次检测的时间
	start time.Time
}

const (
	DefaultPath    = "/_minirpc_/registry"
	DefaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *MiniRegistry {
	return &MiniRegistry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultRegistry = New(DefaultTimeout)

// 加入或更新一个服务器
func (r *MiniRegistry) PutServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.servers[addr] = &ServerItem{
		Addr:  addr,
		start: time.Now(),
	}
}

// 获取活跃的服务器列表，并删除超时的服务器
func (r *MiniRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, server := range r.servers {
		if r.timeout == 0 || time.Since(server.start) < r.timeout {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	return alive
}

const (
	DefaultHTTPFieldGet  = "X-Minirpc-Servers"
	DefaultHTTPFieldPost = "X-Minirpc-Server"
)

// 在指定的路径上注册服务器，默认路径为 /_minirpc_/registry
// 默认通过自定义字段 X-Minirpc-Servers 承载信息
// Get: 返回所有可用的服务器列表，通过字段 X-Minirpc-Servers
// Post: 注册一个新的服务器或者发送心跳，通过字段 X-Minirpc-Server
func (r *MiniRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set(DefaultHTTPFieldGet, strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get(DefaultHTTPFieldPost)
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.PutServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *MiniRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	logrus.Info("minirpc registry listen on ", registryPath)
}

func HandleHTTP() {
	DefaultRegistry.HandleHTTP(DefaultPath)
}

// 向指定的 registry 地址定时发送 server 的心跳包
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 默认是 4 分钟发送一次心跳包
		duration = DefaultTimeout - time.Second
	}

	err := sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// 发送心跳包，registry 包括完整的 URL，如 http://127.0.0.1:8080/_minirpc_/registry
func sendHeartbeat(registry, addr string) error {
	httpClient := new(http.Client)
	req, err := http.NewRequest("POST", registry, nil)
	if err != nil {
		logrus.Error(err)
		return err
	}
	req.Header.Set(DefaultHTTPFieldPost, addr)
	if _, err = httpClient.Do(req); err != nil {
		logrus.Error(err)
		return err
	}
	return nil
}
