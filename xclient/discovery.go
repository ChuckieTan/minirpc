package xclient

import (
	"errors"
	"math/rand"
	"sync"
)

type SelectMode uint8

const (
	SelectMode_Random SelectMode = iota
	SelectMode_RoundRobin
)

type Discovery interface {
	// 刷新服务列表
	Refresh() error
	// 手动更新服务列表
	Update(server []string) error
	// 根据选择的模式选择一个服务
	Get(mode SelectMode) (string, error)
	// 获取所有的服务
	GetAll() ([]string, error)
}

type MultiDiscovery struct {
	// 服务列表
	serverList []string
	// 随机数生成器
	r *rand.Rand
	// 所有服务的互斥锁
	mu sync.RWMutex
	// 记录轮询算法当前选择的服务
	index int
}

func NewMultiDiscovery(serverList []string) *MultiDiscovery {
	return &MultiDiscovery{
		serverList: serverList,
		r:          rand.New(rand.NewSource(0)),
	}
}

// 目前不支持自动刷新，需要调用 Update 手动刷新
func (d *MultiDiscovery) Refresh() error {
	return nil
}

func (d *MultiDiscovery) Update(serverList []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.serverList = serverList
	return nil
}

func (d *MultiDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.serverList) == 0 {
		return "", errors.New("no avaliable server")
	}
	switch mode {
	case SelectMode_Random:
		return d.getRandom(), nil
	case SelectMode_RoundRobin:
		return d.getRoundRobin(), nil
	default:
		return "", errors.New("unknown select mode")
	}
}

func (d *MultiDiscovery) getRandom() string {
	if len(d.serverList) == 0 {
		return ""
	}
	return d.serverList[d.r.Intn(len(d.serverList))]
}

func (d *MultiDiscovery) getRoundRobin() string {
	if len(d.serverList) == 0 {
		return ""
	}
	index := d.index
	d.index = (d.index + 1) % len(d.serverList)
	return d.serverList[index]
}

func (d *MultiDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.serverList))
	copy(servers, d.serverList)
	return servers, nil
}
