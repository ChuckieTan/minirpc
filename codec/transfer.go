package codec

import "reflect"

const MagicNumber = 0x065279

type Option struct {
	MagicNumber int
	CodecType   Type
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   GobType,
}

type Request struct {
	// 服务名和方法名，格式为 "Service.Method"
	ServiceMethod string
	// 请求的序号，用来区分不同的请求
	Seq uint64
	// 远程调用方法需要的参数
	Args reflect.Value
}

type Response struct {
	// 请求的序号，用来区分不同的请求
	Seq uint64
	// 远程调用方法返回的结果
	Reply reflect.Value
	// 错误信息
	Err string
}
