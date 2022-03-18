package minirpc

import (
	"go/ast"
	"reflect"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// 被注册的方法只能有两个参数
// 第一个是实际的参数，第二个是指针类型，表示返回值
type methodType struct {
	// 要调用的方法
	method reflect.Method
	// 参数的类型
	ArgType reflect.Type
	// 返回值的类型
	ReplyType reflect.Type
	// 方法被调用的次数
	numCalls uint64
}

// 返回方法被调用的次数，通过 CAS 机制保证返回的过程中不会被修改
func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// new 一个方法的参数类型
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// new 一个方法的返回值类型
func (m *methodType) newReply() reflect.Value {
	reply := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		reply.Elem().Set(reflect.MakeMap(m.ReplyType))
	case reflect.Slice:
		reply.Elem().Set(reflect.MakeSlice(m.ReplyType, 0, 0))
	}
	return reply
}

// 存储一个被注册的类型及其可调用的方法
type service struct {
	// 注册的结构体的名字
	name string
	// 注册的结构体的类型
	typ reflect.Type
	// 注册的结构体本身，在调用其方法时需要做为第一个参数传入
	rcvr reflect.Value
	// 存储结构体所有符合条件的方法
	method map[string]*methodType
}

func newService(rcvr interface{}) *service {
	var svc = new(service)
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	if !ast.IsExported(svc.name) {
		logrus.Fatalf("%s is not exported", svc.name)
	}
	svc.registerMethods()
	return svc
}

// 注册此服务的结构体的所有方法
func (svc *service) registerMethods() {
	svc.method = make(map[string]*methodType)
	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// 忽略不可导出的方法，即首字母不是大写的方法
		if !ast.IsExported(mname) {
			continue
		}
		// 忽略不是三个参数的方法
		// 其中第一个参数一定是它本身，第二个参数是指针类型，第三个参数是返回值
		if mtype.NumIn() != 3 {
			continue
		}
		// 忽略返回值数量不为一的方法
		if mtype.NumOut() != 1 {
			continue
		}
		// 忽略返回值不是 error 的方法
		if mtype.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mtype.In(1), mtype.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		// replyType 必须为指针类型
		if replyType.Kind() != reflect.Ptr {
			continue
		}
		svc.method[mname] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		logrus.Infof("minirpc server: register method %s.%s", svc.name, mname)
	}
}

// 是可导出的类型，或者是内建类型
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// 调用指定的方法，并写入返回值到 reply 中
func (s *service) call(m *methodType, args, reply reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, args, reply})
	// 返回值只能有一个，即 error
	if len(returnValues) == 1 {
		if returnValues[0].Interface() != nil {
			return returnValues[0].Interface().(error)
		}
	}
	return nil
}
