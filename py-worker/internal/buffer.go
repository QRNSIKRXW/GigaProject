package internal

import "bytes"

type LimitedBuffer struct {
	buf       bytes.Buffer
	maxSize   int
	truncated bool
}

func NewLimitedBuffer(max int) *LimitedBuffer {
	return &LimitedBuffer{maxSize: max}
}

func (l *LimitedBuffer) Write(p []byte) (int, error) {
	// если буфер уже переполнен — игнорируем всё
	if l.buf.Len() >= l.maxSize {
		l.truncated = true
		return len(p), nil
	}

	// если входящие данные превышают лимит — обрезаем
	if len(p)+l.buf.Len() > l.maxSize {
		remain := l.maxSize - l.buf.Len()
		l.buf.Write(p[:remain])
		l.truncated = true
		return len(p), nil
	}

	return l.buf.Write(p)
}

func (l *LimitedBuffer) String() string {
	if l.truncated {
		return l.buf.String() + "\n...output truncated..."
	}
	return l.buf.String()
}
