package protocol

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageWriter_writeBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 24},
	}

	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			buff.Reset()

			err := writer.writeBlob(c.Blob)
			assert.NoError(t, err)

			err = writer.writer.Flush()
			assert.NoError(t, err)

			bytes := buff.Bytes()

			assert.Equal(t, binary.LittleEndian.Uint64(bytes), uint64(len(c.Blob)))
			assert.Equal(t, bytes[8:8+len(c.Blob)], c.Blob)
		})
	}
}

func TestMessageWriter_writeString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello world", 16},
	}

	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			buff.Reset()

			err := writer.writeString(c.String)
			assert.NoError(t, err)

			err = writer.writer.Flush()
			assert.NoError(t, err)

			bytes := buff.Bytes()

			assert.Equal(t, string(bytes[:len(c.String)]), c.String)
			assert.Equal(t, bytes[len(c.String)], byte(0))
		})
	}
}

func TestMessageWriter_writeUint32(t *testing.T) {
	const value = uint32(0xF0CACC1A)

	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	err := writer.writeUint32(value)
	assert.NoError(t, err)
	err = writer.writer.Flush()
	assert.NoError(t, err)

	bytes := buff.Bytes()

	assert.Equal(t, binary.LittleEndian.Uint32(bytes), value)
}

func TestMessageWriter_writeUint64(t *testing.T) {
	const value = uint64(0x600D_F0CACC1A)

	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	err := writer.writeUint64(value)
	assert.NoError(t, err)

	err = writer.writer.Flush()
	assert.NoError(t, err)

	bytes := buff.Bytes()

	assert.Equal(t, binary.LittleEndian.Uint64(bytes), value)
}

func TestMessageWriter_writeNamedValues(t *testing.T) {
	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	timestamp, err := time.ParseInLocation("2006-01-02", "2018-08-01", time.UTC)
	require.NoError(t, err)

	values := NamedValues{
		{Ordinal: 1, Value: int64(123)},
		{Ordinal: 2, Value: float64(3.1415)},
		{Ordinal: 3, Value: true},
		{Ordinal: 4, Value: []byte{1, 2, 3, 4, 5, 6}},
		{Ordinal: 5, Value: "hello"},
		{Ordinal: 6, Value: nil},
		{Ordinal: 7, Value: timestamp},
	}

	err = writer.writeValues(values)
	require.NoError(t, err)

	err = writer.writer.Flush()
	require.NoError(t, err)

	bytes := buff.Bytes()

	assert.Equal(t, 104, len(bytes))
	assert.Equal(t, bytes[0], byte(7))
	assert.Equal(t, bytes[1], byte(Integer))
	assert.Equal(t, bytes[2], byte(Float))
	assert.Equal(t, bytes[3], byte(Boolean))
	assert.Equal(t, bytes[4], byte(Blob))
	assert.Equal(t, bytes[5], byte(Text))
	assert.Equal(t, bytes[6], byte(Null))
	assert.Equal(t, bytes[7], byte(ISO8601))
}

func TestMessageWriter_writeNamedValues32(t *testing.T) {
	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	timestamp, err := time.ParseInLocation("2006-01-02", "2018-08-01", time.UTC)
	require.NoError(t, err)

	values := make(NamedValues, 256)
	values[0] = driver.NamedValue{Ordinal: 1, Value: int64(123)}
	values[1] = driver.NamedValue{Ordinal: 2, Value: float64(3.1415)}
	values[2] = driver.NamedValue{Ordinal: 3, Value: true}
	values[3] = driver.NamedValue{Ordinal: 4, Value: []byte{1, 2, 3, 4, 5, 6}}
	values[4] = driver.NamedValue{Ordinal: 5, Value: "hello"}
	values[5] = driver.NamedValue{Ordinal: 6, Value: nil}
	values[6] = driver.NamedValue{Ordinal: 7, Value: timestamp}
	for i := 7; i < len(values); i++ {
		values[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   nil,
		}
	}

	err = writer.writeValues(values)
	assert.NoError(t, err)

	err = writer.writer.Flush()
	assert.NoError(t, err)

	bytes := buff.Bytes()

	assert.Equal(t, 8+4+256+92+(256-7)*8, len(bytes))
	assert.Equal(t, bytes[0], byte(0))
	assert.Equal(t, bytes[1], byte(1))
	assert.Equal(t, bytes[2], byte(0))
	assert.Equal(t, bytes[3], byte(0))
	assert.Equal(t, bytes[4], byte(Integer))
	assert.Equal(t, bytes[5], byte(Float))
	assert.Equal(t, bytes[6], byte(Boolean))
	assert.Equal(t, bytes[7], byte(Blob))
	assert.Equal(t, bytes[8], byte(Text))
	assert.Equal(t, bytes[9], byte(Null))
	assert.Equal(t, bytes[10], byte(ISO8601))
	for i := 0; i < len(values)-7; i++ {
		assert.Equal(t, bytes[11+i], byte(Null))
	}
}

func BenchmarkMessageWriter_writeString(b *testing.B) {
	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buff.Reset()
		writer.writeString("hello")
		writer.writer.Flush()
	}
}

func BenchmarkMessageWriter_writeUint64(b *testing.B) {
	buff := &bytes.Buffer{}
	writer := NewMessageWriter(buff)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buff.Reset()
		writer.writeUint64(270)
		writer.writer.Flush()
	}
}
