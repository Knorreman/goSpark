package spark

import "context"

type cancelCodec struct {
	ctx context.Context
	RecordCodec
}

func (c cancelCodec) Encode(v any) ([]byte, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	return c.RecordCodec.Encode(v)
}
func (c cancelCodec) Decode(v []byte) (any, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	return c.RecordCodec.Decode(v)
}
