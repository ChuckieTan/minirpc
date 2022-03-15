package codec

import (
	"bufio"
	"encoding/gob"
	"io"

	"github.com/sirupsen/logrus"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (c *GobCodec) Read(v interface{}) error {
	if err := c.dec.Decode(v); err != nil {
		logrus.Errorf("decode error: %v", err)
		return err
	}
	return nil
}

func (c *GobCodec) Write(v interface{}) error {
	if err := c.enc.Encode(v); err != nil {
		logrus.Errorf("encode error: %v", err)
		return err
	}
	return c.buf.Flush()
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
