package xclient

import (
	"minirpc/registry"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type MiniRegistryDiscovery struct {
	// 原始的多服务器选择器，复用了其原来的字段
	*MultiDiscovery
	// registry 地址，包含完整的 URL，如 http://127.0.0.1:2379/_minirpc_/registry
	registry string
	// 服务列表的过期时间
	timeout time.Duration
	// 最后更新服务的时间
	lastUpdate time.Time
}

const (
	defaultUpdateTimeout = time.Second * 10
)

func NewMiniRegistryDiscovery(registry string, timeout time.Duration) *MiniRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	return &MiniRegistryDiscovery{
		MultiDiscovery: NewMultiDiscovery(make([]string, 0)),
		registry:       registry,
		timeout:        timeout,
	}
}

func (d *MiniRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.serverList = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *MiniRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Now().Sub(d.lastUpdate) < d.timeout {
		return nil
	}
	logrus.Info("refresh servers from registry ", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		logrus.Error("rpc registry refresh error: ", err)
	}
	servers := strings.Split(resp.Header.Get(registry.DefaultHTTPFieldGet), ",")
	d.serverList = make([]string, 0)
	for _, server := range servers {
		server = strings.TrimSpace(server)
		if server != "" {
			d.serverList = append(d.serverList, server)
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *MiniRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	addr, err := d.MultiDiscovery.Get(mode)
	return addr, err
}

func (d *MiniRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiDiscovery.GetAll()
}
