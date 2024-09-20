package protocol

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageReader_readString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello!!!", 16},
		{"hello world", 16},
	}

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			buff := &bytes.Buffer{}
			writer := NewMessageWriter(buff)

			writer.writeString(c.String)
			writer.writer.Flush()

			reader := NewMessageReader(buff)
			*reader.limit = int64(buff.Len())

			s, _ := reader.readString()

			assert.Equal(t, s, c.String)
		})
	}
}

func TestMessageReader_readBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 24},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			buff := &bytes.Buffer{}
			writer := NewMessageWriter(buff)

			writer.writeBlob(c.Blob)
			writer.writer.Flush()

			reader := NewMessageReader(buff)
			*reader.limit = int64(buff.Len())

			s, _ := reader.readBlob()

			assert.Equal(t, s, c.Blob)
		})
	}
}

// The overflowing string ends exactly at word boundary.
func TestMessageReader_readString_Overflow_WordBoundary(t *testing.T) {
	buff := []byte{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
		'i', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
		0, 0, 0, 0, 0, 0, 0,
	}
	reader := NewMessageReader(
		bytes.NewReader(buff),
	)
	*reader.limit = int64(len(buff))

	s, _ := reader.readString()
	assert.Equal(t, "abcdefghilmnopqr", s)

	assert.Equal(t, int64(0), *reader.limit)
}

func BenchmarkMessageReader_readBlob(b *testing.B) {
	makeBlobReader := func(size int) *bytes.Reader {
		buff := &bytes.Buffer{}
		writer := NewMessageWriter(buff)

		writer.writeBlob(make([]byte, size))
		writer.writer.Flush()

		return bytes.NewReader(buff.Bytes())
	}

	for _, size := range []int{16, 64, 256, 1024, 4096, 8096} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			blob := makeBlobReader(size)
			reader := NewMessageReader(blob)

			for i := 0; i < b.N; i++ {
				blob.Seek(0, io.SeekStart)
				reader.readBlob()
			}
		})
	}
}
