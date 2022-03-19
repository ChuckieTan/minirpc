package codec

import (
	"bufio"
	"encoding/gob"
	"io"

	"github.com/sirupsen/logrus"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.ReadWriter
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	writeBuf := bufio.NewWriter(conn)
	readBuf := bufio.NewReader(conn)
	buf := bufio.NewReadWriter(readBuf, writeBuf)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(buf),
		enc:  gob.NewEncoder(buf),
	}
}

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		logrus.Error("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		logrus.Error("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
