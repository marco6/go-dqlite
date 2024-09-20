package protocol

import (
	"bufio"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

type messageReader struct {
	reader *bufio.Reader
	limit  *int64
}

func NewMessageReader(reader io.Reader) *messageReader {
	limitedReader := &io.LimitedReader{
		R: reader,
		N: 0,
	}
	return &messageReader{
		reader: bufio.NewReader(limitedReader),
		limit:  &limitedReader.N,
	}
}

func (mr *messageReader) ReadNode() (uint64, string, error) {
	_, err := mr.readHeader(ResponseNode)
	if err != nil {
		return 0, "", err
	}

	id, err := mr.readUint64()
	if err != nil {
		return 0, "", err
	}
	address, err := mr.readString()
	if err != nil {
		return 0, "", err
	}

	return id, address, nil
}

func (mr *messageReader) ReadNodeLegacy() (string, error) {
	_, err := mr.readHeader(ResponseNodeLegacy)
	if err != nil {
		return "", err
	}

	return mr.readString()
}

func (mr *messageReader) ReadWelcome() (uint64, error) {
	_, err := mr.readHeader(ResponseWelcome)
	if err != nil {
		return 0, err
	}

	return mr.readUint64()
}

func (mr *messageReader) ReadRows() (*RowsReader, error) {
	rows := &RowsReader{
		reader: mr,
	}
	if err := rows.readColumns(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (mr *messageReader) ReadDb() (uint32, error) {
	_, err := mr.readHeader(ResponseDb)
	if err != nil {
		return 0, err
	}

	dbId, err := mr.readUint32()
	if err != nil {
		return 0, err
	}
	return dbId, mr.discardPadding()
}

func (mr *messageReader) ReadStmt() (uint32, uint64, error) {
	_, err := mr.readHeader(ResponseStmt)
	if err != nil {
		return 0, 0, err
	}

	_, err = mr.reader.Discard(4) // db id
	if err != nil {
		return 0, 0, err
	}
	stmtId, err := mr.readUint32()
	if err != nil {
		return 0, 0, err
	}
	params, err := mr.readUint64()
	if err != nil {
		return 0, 0, err
	}

	return stmtId, params, nil
}

func (mr *messageReader) ReadResult() (*Result, error) {
	_, err := mr.readHeader(ResponseResult)
	if err != nil {
		return nil, err
	}

	lastInsertId, err := mr.readInt64()
	if err != nil {
		return nil, err
	}
	rowsAffected, err := mr.readInt64()
	if err != nil {
		return nil, err
	}

	return &Result{
		lastInsertId: lastInsertId,
		rowsAffected: rowsAffected,
	}, nil
}

func (mr *messageReader) ReadAck() error {
	_, err := mr.readHeader(ResponseEmpty)
	if err != nil {
		return err
	}
	return mr.discardRemaining()
}

func (mr *messageReader) ReadClusterV1() (Nodes, error) {
	_, err := mr.readHeader(ResponseNodes)
	if err != nil {
		return nil, err
	}
	n, err := mr.readUint64()
	if err != nil {
		return nil, err
	}
	servers := make(Nodes, n)
	for i := 0; i < int(n); i++ {
		id, err := mr.readUint64()
		if err != nil {
			return nil, err
		}
		address, err := mr.readString()
		if err != nil {
			return nil, err
		}
		role, err := mr.readUint64()
		if err != nil {
			return nil, err
		}

		servers[i].ID = id
		servers[i].Address = address
		servers[i].Role = NodeRole(role)
	}

	return servers, nil
}

func (mr *messageReader) ReadFiles() ([]File, error) {
	_, err := mr.readHeader(ResponseFiles)
	if err != nil {
		return nil, err
	}
	n, err := mr.readUint64()
	if err != nil {
		return nil, err
	}
	files := make([]File, n)
	for i := range files {
		name, err := mr.readString()
		if err != nil {
			return nil, err
		}
		data, err := mr.readBlob()
		if err != nil {
			return nil, err
		}
		files[i] = File{
			Name: name,
			Data: data,
		}
	}

	return files, nil
}

func (mr *messageReader) ReadMetadataV0() (*NodeMetadata, error) {
	_, err := mr.readHeader(ResponseMetadata)
	if err != nil {
		return nil, err
	}

	failureDomain, err := mr.readUint64()
	if err != nil {
		return nil, err
	}
	weight, err := mr.readUint64()
	if err != nil {
		return nil, err
	}

	return &NodeMetadata{
		FailureDomain: failureDomain,
		Weight:        weight,
	}, nil
}

func (mr *messageReader) readHeader(requiredType ResponseType) (uint8, error) {
	if *mr.limit != 0 {
		return 0, fmt.Errorf("unread message in stream")
	}
	*mr.limit = messageHeaderSize
	buff, err := mr.reader.Peek(8)
	if err != nil {
		return 0, err
	}
	words := binary.LittleEndian.Uint32(buff)
	mtype, schema := ResponseType(buff[4]), buff[5]
	mr.reader.Discard(8) // Can't fail
	*mr.limit = int64(words) * messageWordSize

	if mtype == ResponseFailure {
		return 0, mr.readError()
	}
	if mtype != requiredType {
		return 0, fmt.Errorf("decode %s: unexpected type %s", requiredType.String(), mtype.String())
	}

	return schema, nil
}

func (mr *messageReader) readError() error {
	code, err := mr.readUint64()
	if err != nil {
		return err
	}
	descr, err := mr.readString()
	if err != nil {
		return err
	}
	return ErrRequest{
		Code:        code,
		Description: descr,
	}
}

func (mr *messageReader) readColumnNames() ([]string, error) {
	n, err := mr.readUint64()
	if err != nil {
		return nil, err
	}
	cols := make([]string, n)
	for i := range cols {
		col, err := mr.readString()
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	return cols, nil
}

type EndMarker uint64

func (mr *messageReader) readEndMarker() (bool, error) {
	const (
		done = 0xffffffffffffffff
		more = 0xeeeeeeeeeeeeeeee
	)
	if mr.remaining() != messageWordSize {
		return false, nil // Not at the end
	}
	if marker, err := mr.readUint64(); err != nil {
		return false, err
	} else if marker == done {
		return false, nil
	} else if marker == more {
		return true, nil
	} else {
		return false, fmt.Errorf("unexpected end of message")
	}
}

func (mr *messageReader) readTypes(types []ColumnType) error {
	typesSize := alignUp((len(types)+1)/2, messageWordSize)
	buff, err := mr.reader.Peek(typesSize)
	if err != nil {
		return err
	}

	i := 0
	for _, b := range buff {
		if i < len(types) {
			types[i] = ColumnType(b & 0x0f)
		}
		i++
		if i < len(types) {
			types[i] = ColumnType(b >> 4)
		}
		i++
	}

	mr.reader.Discard(typesSize) // Can't fail
	return nil
}

func (mr *messageReader) readString() (string, error) {
	s, err := mr.reader.ReadString(0)
	if err != nil {
		return "", err
	}
	err = mr.discardPadding()
	if err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

func (mr *messageReader) discardPadding() error {
	_, err := mr.reader.Discard(mr.remaining() & (messageHeaderSize - 1))
	return err
}

func (mr *messageReader) discardRemaining() error {
	_, err := mr.reader.Discard(mr.remaining())
	return err
}

func (mr *messageReader) remaining() int {
	return mr.reader.Buffered() + int(*mr.limit)
}

func (mr *messageReader) discardColumnNames() error {
	n, err := mr.readUint64()
	if err != nil {
		return err
	}
	for i := 0; i < int(n); i++ {
		if err := mr.discardString(); err != nil {
			return err
		}
	}
	return nil
}

func (mr *messageReader) discardString() error {
	err := bufio.ErrBufferFull
	for err == bufio.ErrBufferFull {
		_, err = mr.reader.ReadSlice(0)
	}
	if err != nil {
		return err
	}
	return mr.discardPadding()
}

func (mr *messageReader) readBlob() ([]byte, error) {
	len, err := mr.readUint64()
	if err != nil {
		return nil, err
	}
	result := make([]byte, len)
	_, err = io.ReadFull(mr.reader, result)
	if err != nil {
		return nil, err
	}
	err = mr.discardPadding()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (mr *messageReader) readUint64() (uint64, error) {
	buff, err := mr.reader.Peek(8)
	if err != nil {
		return 0, err
	}
	result := binary.LittleEndian.Uint64(buff)
	mr.reader.Discard(8) // Can't fail.
	return result, nil
}

func (mr *messageReader) readInt64() (int64, error) {
	v, err := mr.readUint64()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

func (mr *messageReader) readUint32() (uint32, error) {
	buff, err := mr.reader.Peek(4)
	if err != nil {
		return 0, err
	}
	result := binary.LittleEndian.Uint32(buff)
	mr.reader.Discard(4) // Can't fail.
	return result, nil
}

func (mr *messageReader) readFloat64() (float64, error) {
	v, err := mr.readUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

type RowsReader struct {
	reader    *messageReader
	typesRead bool
	columns   []string
	types     []ColumnType
}

var _ driver.Rows = &RowsReader{}

func (rows *RowsReader) Columns() []string {
	return append([]string{}, rows.columns...)
}

func (rows *RowsReader) ColumnType(i int) (ColumnType, error) {
	if err := rows.readTypes(); err != nil {
		return 0, err
	}

	if i < 0 || i >= len(rows.types) {
		return 0, fmt.Errorf("out of bound")
	}

	return rows.types[i], nil
}

// ColumnTypeDatabaseTypeName implements RowsColumnTypeDatabaseTypeName.
// warning: not thread safe
func (rows *RowsReader) ColumnTypeDatabaseTypeName(i int) string {
	typ, err := rows.ColumnType(i)
	if err != nil {
		// a panic here doesn't really help,
		// as an empty column type is not the end of the world
		return ""
	}
	return typ.String()
}

func (rows *RowsReader) Next(dest []driver.Value) error {
	if len(rows.columns) != len(dest) {
		return fmt.Errorf("requested %d columns, but the response only contains %d", len(dest), len(rows.columns))
	}

	if err := rows.readTypes(); err != nil {
		return err
	}

	for i := range rows.types {
		var err error
		switch rows.types[i] {
		case Integer:
			dest[i], err = rows.reader.readInt64()
		case Float:
			dest[i], err = rows.reader.readFloat64()
		case Blob:
			dest[i], err = rows.reader.readBlob()
		case Text:
			dest[i], err = rows.reader.readString()
		case Null:
			_, err = rows.reader.reader.Discard(8)
			dest[i] = nil
		case UnixTime:
			var timestamp int64
			timestamp, err = rows.reader.readInt64()
			if err != nil {
				break
			}
			dest[i] = time.Unix(timestamp, 0)
		case ISO8601:
			var timeString string
			timeString, err = rows.reader.readString()
			if err != nil {
				break
			}
			if timeString == "" {
				dest[i] = nil
				break
			}
			var t time.Time
			timeString = strings.TrimSuffix(timeString, "Z")
			for _, format := range iso8601Formats {
				if t, err = time.ParseInLocation(format, timeString, time.UTC); err == nil {
					break
				}
			}
			if err != nil {
				return err
			}

			dest[i] = t
		case Boolean:
			dest[i], err = rows.reader.readInt64()
		default:
			panic("unknown data type")
		}
		if err != nil {
			return err
		}
	}

	rows.typesRead = false
	return nil
}

func (rows *RowsReader) Close() error {
	if rows.reader == nil {
		return nil
	}
	defer func() {
		rows.reader = nil
	}()

	for rows.reader.remaining() != 0 {
		_, err := rows.reader.reader.Discard(rows.reader.remaining() - messageWordSize)
		if err != nil {
			return err
		}
		more, err := rows.reader.readEndMarker()
		if err != nil {
			return err
		}
		if !more {
			break
		}
		_, err = rows.reader.readHeader(ResponseRows)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rows *RowsReader) readTypes() error {
	if rows.typesRead {
		return nil
	}

	if fetchMore, err := rows.reader.readEndMarker(); err != nil {
		return err
	} else if fetchMore {
		if err := rows.readColumns(); err != nil {
			return err
		}
		return rows.readTypes()
	}

	if rows.types == nil {
		rows.types = make([]ColumnType, len(rows.columns))
	}
	if err := rows.reader.readTypes(rows.types); err != nil {
		return err
	}
	rows.typesRead = true
	return nil
}

func (rows *RowsReader) readColumns() error {
	_, err := rows.reader.readHeader(ResponseRows)
	if err != nil {
		return err
	}

	if rows.columns != nil {
		// It is expected for columns not to change
		return rows.reader.discardColumnNames()
	}

	rows.columns, err = rows.reader.readColumnNames()
	if err != nil {
		return err
	}
	return nil
}

func measureNamedValues(args []driver.NamedValue) int {
	result := len(args)
	if len(args) > math.MaxUint8 {
		result += 4
	} else if len(args) > 0 {
		result += 1
	}
	result = alignUp(result, messageWordSize)

	for i := range args {
		switch value := args[i].Value.(type) {
		case int64, float64, bool, nil:
			result += messageWordSize
		case []byte:
			result += messageWordSize + len(value)
		case string:
			result += len(value) + 1
		case time.Time:
			result += len(iso8601Strict) + measureYear(value.Year()) - 4
		default:
			panic("unsupported value type")
		}
		result = alignUp(result, messageWordSize)
	}
	return result
}

func measureYear(i int) int {
	if i < 0 {
		return measureYear(-i) + 1
	}
	if i <= 9999 {
		return 4
	}
	if i >= 1e18 {
		return 19
	}
	x, count := 10000, 4
	for x <= i {
		x *= 10
		count++
	}
	return count
}
