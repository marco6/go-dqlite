package protocol

import (
	"database/sql/driver"
)

// NamedValues is a type alias of a slice of driver.NamedValue. It's used by
// schema.sh to generate encoding logic for statement parameters.
type NamedValues = []driver.NamedValue

type NamedValues32 = []driver.NamedValue

// Nodes is a type alias of a slice of NodeInfo. It's used by schema.sh to
// generate decoding logic for the heartbeat response.
type Nodes []NodeInfo

type NodeMetadata struct {
	FailureDomain uint64
	Weight        uint64
}

// Result holds the result of a statement.
type Result struct {
	lastInsertId int64
	rowsAffected int64
}

var _ driver.Result = &Result{}

func (r *Result) LastInsertId() (int64, error) { return r.lastInsertId, nil }
func (r *Result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type File struct {
	Name string
	Data []byte
}

const (
	messageWordSize                 = 8
	messageWordBits                 = messageWordSize * 8
	messageHeaderSize               = messageWordSize
	messageMaxConsecutiveEmptyReads = 100
)

const iso8601Strict = "2006-01-02 15:04:05.000000000-07:00"

var iso8601Formats = []string{
	// By default, store timestamps with whatever timezone they come with.
	// When parsed, they will be returned with the same timezone.
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// alignUp rounds n up to a multiple of a. a must be a power of 2.
func alignUp(n, a int) int {
	return (n + a - 1) &^ (a - 1)
}
