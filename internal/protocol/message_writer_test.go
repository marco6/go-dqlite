package protocol_test

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/canonical/go-dqlite/internal/protocol"
)

func TestQuerySQL(t *testing.T) {
	buff := &bytes.Buffer{}
	messageWriter := protocol.NewMessageWriter(buff)
	values := []driver.NamedValue{
		{
			Ordinal: 1,
			Value:   []byte{'a', 'b', 'c'},
		},
	}
	if err := messageWriter.WriteExecSQL(1, "INSERT INTO test(data) VALUES(?)", values); err != nil {
		t.Error(err)
	}

	message := &protocol.Message{}
	message.Init(4096)
	protocol.EncodeExecSQLV0(message, 1, "INSERT INTO test(data) VALUES(?)", values)

	if err := checkMessage(buff.Bytes(), message); err != nil {
		t.Error(err)
	}
}

func checkMessage(buff []byte, message *protocol.Message) error {
	header, body := buff[:8], buff[8:]
	if !bytes.Equal(message.Header(), header) {
		return fmt.Errorf("header mismatch: expected %v got %v", message.Header(), header)
	}

	expectedBody, expectedLength := message.Body()
	if !bytes.Equal(expectedBody[:expectedLength], body) {
		return fmt.Errorf("body mismatch: expected %v got %v", expectedBody[:expectedLength], body)
	}

	return nil
}
