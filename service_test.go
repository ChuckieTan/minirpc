package minirpc

import (
	"reflect"
	"testing"
)

type Foo struct{}

type Args struct {
	A, B int
}

// 可导出方法
func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

// 不可导出方法
func (f Foo) sum(args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func _assert(t *testing.T, ok bool, format string, args ...interface{}) {
	if !ok {
		t.Fatalf(format, args...)
	}
}

func TestNewService(t *testing.T) {
	_assert(t, newService(Foo{}) != nil, "NewService failed")
	svc := newService(Foo{})
	_assert(t, svc.name == "Foo", "NewService failed")
	_assert(t, len(svc.method) == 1, "NewService failed")
	_assert(t, svc.method["Sum"] != nil, "NewService failed")
	_assert(t, svc.method["sum"] == nil, "NewService failed")
}

func TestMethodType_Call(t *testing.T) {
	svc := newService(Foo{})
	mType := svc.method["Sum"]
	args := mType.newArgv()
	reply := mType.newReply()
	args.Set(reflect.ValueOf(Args{1, 2}))
	err := svc.call(mType, args, reply)
	_assert(t, err == nil, "call failed")
	_assert(t, *reply.Interface().(*int) == 3, "call failed")
}
