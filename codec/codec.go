package codec

import (
	"io"
)

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // TODO
)

type Header struct {
	// 要远程调用的方法名，格式为"Service.Method"
	ServiceMethod string
	// 远程调用的序号，用来区分不同的调用
	Seq   uint64
	Error string
}

// 编码器接口，用来编码报文
// 不同的编码方式需要有不同的编码器实现
type Codec interface {
	// 关闭流
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	// 发送信息，可以为 Request 或者 Response
	Write(*Header, interface{}) error
}

// 编码器的构造函数类型
type NewCodecFunc func(io.ReadWriteCloser) Codec

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
	// NewCodecFuncMap[JsonType] = NewJsonCodec
}
