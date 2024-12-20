package producer

type NullWriter struct {
}

func (u NullWriter) Write(p []byte) (n int, err error) {
	return 0, err
}
