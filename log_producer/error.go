package log_producer

type BufferBusy struct{}

func (b BufferBusy) Error() string {

	msg := "producer buffer busy, please increase producer queue buffer"

	return msg
}
