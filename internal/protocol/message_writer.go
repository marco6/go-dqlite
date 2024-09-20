package protocol

import (
	"bufio"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
	"unsafe"
)

type messageWriter struct {
	writer *bufio.Writer
}

func NewMessageWriter(writer io.Writer) *messageWriter {
	return &messageWriter{
		writer: bufio.NewWriter(writer),
	}
}

func (mw *messageWriter) WriteAdd(id uint64, address string) error {
	messageSize := 8 + // id
		stringSize(address)

	if err := mw.writeHeader(messageSize, RequestAdd, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(uint64(id)); err != nil {
		return err
	}
	if err := mw.writeString(address); err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WriteLeader() error {
	const messageSize = messageWordSize
	err := mw.writeHeader(messageSize, RequestLeader, 0)
	if err != nil {
		return err
	}
	err = mw.writeUint64(0)
	if err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WriteTransfer(id uint64) error {
	const messageSize = messageWordSize
	if err := mw.writeHeader(messageSize, RequestTransfer, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(id); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteCluster(formatVersion ClusterFormat) error {
	const messageSize = messageWordSize
	if err := mw.writeHeader(messageSize, RequestCluster, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(uint64(formatVersion)); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteDescribe(formatVersion DescribeFormat) error {
	const messageSize = messageWordSize
	if err := mw.writeHeader(messageSize, RequestDescribe, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(uint64(formatVersion)); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteAssign(id uint64, role uint64) error {
	const messageSize = 2 * messageWordSize
	if err := mw.writeHeader(messageSize, RequestAssign, 0); err != nil {
		return err
	}

	if err := mw.writeUint64(id); err != nil {
		return err
	}
	if err := mw.writeUint64(role); err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WriteRemove(id uint64) error {
	const messageSize = messageWordSize
	if err := mw.writeHeader(messageSize, RequestRemove, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(id); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteWeight(weight uint64) error {
	const messageSize = messageWordSize
	if err := mw.writeHeader(messageSize, RequestWeight, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(weight); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteClient(id uint64) error {
	const messageSize = messageWordSize
	if err := mw.writeHeader(messageSize, RequestClient, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(id); err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WriteDump(name string) error {
	messageSize := stringSize(name)
	if err := mw.writeHeader(messageSize, RequestDump, 0); err != nil {
		return err
	}
	if err := mw.writeString(name); err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WriteQuerySQL(db uint32, query string, args []driver.NamedValue) error {
	return mw.writeQuerySQL(RequestQuerySQL, db, query, args)
}

func (mw *messageWriter) WriteExecSQL(db uint32, query string, args []driver.NamedValue) error {
	return mw.writeQuerySQL(RequestExecSQL, db, query, args)
}

func (mw *messageWriter) writeQuerySQL(mtype RequestType, db uint32, query string, args []driver.NamedValue) error {
	messageSize := 8 + // database id
		stringSize(query) +
		measureNamedValues(args)

	version := uint8(0)
	if len(args) > math.MaxUint8 {
		version = 1
	}
	if err := mw.writeHeader(messageSize, mtype, version); err != nil {
		return err
	}
	if err := mw.writeUint64(uint64(db)); err != nil {
		return err
	}
	if err := mw.writeString(query); err != nil {
		return err
	}
	if err := mw.writeValues(args); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteQuery(db, query uint32, args []driver.NamedValue) error {
	return mw.writeQuery(RequestQuery, db, query, args)
}

func (mw *messageWriter) WriteExec(db, query uint32, args []driver.NamedValue) error {
	return mw.writeQuery(RequestExec, db, query, args)
}

func (mw *messageWriter) writeQuery(mtype RequestType, db, query uint32, args []driver.NamedValue) error {
	messageSize := 4 + // database id
		4 + // query id
		measureNamedValues(args)

	version := uint8(0)
	if len(args) > math.MaxUint8 {
		version = 1
	}
	if err := mw.writeHeader(messageSize, mtype, version); err != nil {
		return err
	}
	if err := mw.writeUint32(db); err != nil {
		return err
	}
	if err := mw.writeUint32(query); err != nil {
		return err
	}
	if err := mw.writeValues(args); err != nil {
		return err
	}
	return mw.writer.Flush()
}

func (mw *messageWriter) WriteFinalize(dbId, stmtId uint32) error {
	const messageSize = 8
	err := mw.writeHeader(messageSize, RequestFinalize, 0)
	if err != nil {
		return err
	}
	err = mw.writeUint32(dbId)
	if err != nil {
		return err
	}
	err = mw.writeUint32(stmtId)
	if err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WriteOpen(name string, flags uint64, vfs string) error {
	messageSize := stringSize(name) +
		8 + // flags
		stringSize(vfs)

	if err := mw.writeHeader(messageSize, RequestOpen, 0); err != nil {
		return err
	}
	if err := mw.writeString(name); err != nil {
		return err
	}
	if err := mw.writeUint64(flags); err != nil {
		return err
	}
	if err := mw.writeString(vfs); err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) WritePrepare(db uint32, sql string) error {
	messageSize := 8 + // db
		stringSize(sql)

	if err := mw.writeHeader(messageSize, RequestPrepare, 0); err != nil {
		return err
	}
	if err := mw.writeUint64(uint64(db)); err != nil {
		return err
	}
	if err := mw.writeString(sql); err != nil {
		return err
	}

	return mw.writer.Flush()
}

func (mw *messageWriter) writeHeader(messageSize int, mtype RequestType, version uint8) error {
	if messageSize < 0 {
		return fmt.Errorf("message size is negative")
	}
	if int64(messageSize) > 4*math.MaxUint32 {
		return fmt.Errorf("message size is too big")
	}
	if messageSize&(messageWordSize-1) != 0 {
		return fmt.Errorf("message size is not aligned")
	}
	header := mw.writer.AvailableBuffer()
	header = binary.LittleEndian.AppendUint32(header, uint32(messageSize/messageWordSize))
	header = append(header, byte(mtype), version, 0, 0)
	_, err := mw.writer.Write(header)
	return err
}

func (mw *messageWriter) writeValues(args []driver.NamedValue) error {
	l := len(args)
	if l == 0 {
		return nil
	}

	header := mw.writer.AvailableBuffer()

	// FIXME ma ha senso questa complicazione?
	if len(args) > math.MaxUint8 {
		header = binary.LittleEndian.AppendUint32(header, uint32(len(args)))
	} else {
		header = append(header, uint8(len(args)))
	}

	for i := range args {
		switch args[i].Value.(type) {
		case int64:
			header = append(header, byte(Integer))
		case float64:
			header = append(header, byte(Float))
		case bool:
			header = append(header, byte(Boolean))
		case []byte:
			header = append(header, byte(Blob))
		case string:
			header = append(header, byte(Text))
		case nil:
			header = append(header, byte(Null))
		case time.Time:
			header = append(header, byte(ISO8601))
		default:
			panic("unsupported value type")
		}
	}
	if _, err := mw.writer.Write(appendPadding(header)); err != nil {
		return err
	}

	for i := range args {
		var err error
		switch v := args[i].Value.(type) {
		case int64:
			err = mw.writeInt64(v)
		case float64:
			err = mw.writeFloat64(v)
		case bool:
			if v {
				err = mw.writeUint64(1)
			} else {
				err = mw.writeUint64(0)
			}
		case []byte:
			err = mw.writeBlob(v)
		case string:
			err = mw.writeString(v)
		case nil:
			err = mw.writeInt64(0)
		case time.Time:
			err = mw.writeTimestamp(v)
		default:
			panic("unsupported value type")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (mw *messageWriter) writeTimestamp(t time.Time) error {
	buff := mw.writer.AvailableBuffer()
	buff = t.AppendFormat(buff, iso8601Strict)
	_, err := mw.writer.Write(appendPadding(buff))
	return err
}

func (mw *messageWriter) writeString(v string) error {
	n, err := mw.writer.Write(unsafe.Slice(unsafe.StringData(v), len(v)))
	if err != nil {
		return err
	}

	err = mw.writer.WriteByte(0)
	if err != nil {
		return err
	}
	return mw.writePadding(n + 1)
}

func (mw *messageWriter) writePadding(written int) error {
	paddingSize := alignUp(written, messageWordSize) - written
	if paddingSize == 0 {
		return nil
	}
	if mw.writer.Available() == 0 {
		if err := mw.writer.Flush(); err != nil {
			return err
		}
	}
	buff := mw.writer.AvailableBuffer()
	buff = append(buff, make([]byte, paddingSize)...)
	_, err := mw.writer.Write(buff)
	return err
}

func (mw *messageWriter) writeBlob(v []byte) error {
	err := mw.writeUint64(uint64(len(v)))
	if err != nil {
		return err
	}
	_, err = mw.writer.Write(v)
	if err != nil {
		return err
	}
	return mw.writePadding(len(v))
}

func (mw *messageWriter) writeUint64(v uint64) error {
	buff := mw.writer.AvailableBuffer()
	buff = binary.LittleEndian.AppendUint64(buff, v)
	_, err := mw.writer.Write(buff)
	return err
}

func (mw *messageWriter) writeInt64(v int64) error {
	return mw.writeUint64(uint64(v))
}

func (mw *messageWriter) writeUint32(v uint32) error {
	buff := mw.writer.AvailableBuffer()
	buff = binary.LittleEndian.AppendUint32(buff, v)
	_, err := mw.writer.Write(buff)
	return err
}

func (mw *messageWriter) writeFloat64(v float64) error {
	return mw.writeUint64(math.Float64bits(v))
}

func appendPadding(buff []byte) []byte {
	return append(buff, make([]byte, alignUp(len(buff), messageWordSize)-len(buff))...)
}

func stringSize(s string) int {
	return alignUp(len(s)+1, messageWordSize)
}
