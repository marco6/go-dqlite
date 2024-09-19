package protocol

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Protocol sends and receive the dqlite message on the wire.
type Protocol struct {
	version uint64   // Protocol version
	conn    net.Conn // Underlying network connection. TODO: remove. This belongs to `Conn`
	reader  *messageReader
	writer  *messageWriter
	closeCh chan struct{} // Stops the heartbeat when the connection gets closed
	mu      sync.Mutex    // Serialize requests
	netErr  error         // A network error occurred
}

func NewProtocol(conn net.Conn, version uint64) (*Protocol, error) {
	writer := NewMessageWriter(conn)
	if err := writer.writeUint64(version); err != nil {
		return nil, err
	}
	if err := writer.writer.Flush(); err != nil {
		return nil, err
	}

	protocol := &Protocol{
		version: version,
		conn:    conn,
		// FIXME: I think that the best size here depends on the type of the
		// underlying connection. If it is a local thing, then page size (4KiB)
		// is probably the right choice. However, I would bet the right buffer
		// size for networking would be the MTU size (or maybe the max payload
		// size for the IP packet) or some multiple of it.
		reader:  NewMessageReader(conn),
		writer:  writer,
		closeCh: make(chan struct{}),
	}

	return protocol, nil
}

// Call invokes a dqlite RPC, sending a request message and receiving a
// response message.
func (p *Protocol) Call(ctx context.Context, request, response *Message) (err error) {
	// We need to take a lock since the dqlite server currently does not
	// support concurrent requests.
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	var budget time.Duration

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		budget = time.Until(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if err = p.send(request); err != nil {
		return errors.Wrapf(err, "call %s (budget %s): send", requestDesc(request.mtype), budget)
	}

	if err = p.recv(response); err != nil {
		return errors.Wrapf(err, "call %s (budget %s): receive", requestDesc(request.mtype), budget)
	}

	return
}

func (p *Protocol) Leader(ctx context.Context) (id uint64, address string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return 0, "", p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if err := p.writer.WriteLeader(); err != nil {
		return 0, "", err
	}

	if p.version == VersionLegacy {
		address, err := p.reader.ReadNodeLegacy()
		if err != nil {
			return 0, "", err
		}
		return 0, address, err
	}

	return p.reader.ReadNode()
}

func (p *Protocol) RegisterClient(ctx context.Context, id uint64) (heartbeatTimeout uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return 0, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if err := p.writer.WriteClient(id); err != nil {
		return 0, err
	}

	return p.reader.ReadWelcome()
}

func (p *Protocol) QuerySQL(ctx context.Context, db uint32, query string, args []driver.NamedValue) (rows *RowsReader, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return nil, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if int64(len(args)) > math.MaxUint32 {
		return nil, fmt.Errorf("too many parameters (%d)", len(args))
	}

	if err := p.writer.WriteQuerySQL(db, query, args); err != nil {
		return nil, err
	}

	return p.reader.ReadRows()
}

func (p *Protocol) ExecSQL(ctx context.Context, db uint32, query string, args []driver.NamedValue) (result *Result, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return nil, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if int64(len(args)) > math.MaxUint32 {
		return nil, fmt.Errorf("too many parameters (%d)", len(args))
	}

	if err := p.writer.WriteExecSQL(db, query, args); err != nil {
		return nil, err
	}

	return p.reader.ReadResult()
}

func (p *Protocol) Query(ctx context.Context, db, query uint32, args []driver.NamedValue) (rows *RowsReader, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return nil, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if int64(len(args)) > math.MaxUint32 {
		return nil, fmt.Errorf("too many parameters (%d)", len(args))
	}

	if err := p.writer.WriteQuery(db, query, args); err != nil {
		return nil, err
	}

	return p.reader.ReadRows()
}

func (p *Protocol) Exec(ctx context.Context, db, query uint32, args []driver.NamedValue) (result *Result, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return nil, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	if int64(len(args)) > math.MaxUint32 {
		return nil, fmt.Errorf("too many parameters (%d)", len(args))
	}

	if err := p.writer.WriteExec(db, query, args); err != nil {
		return nil, err
	}

	return p.reader.ReadResult()
}

func (p *Protocol) Open(ctx context.Context, name string, flags uint64, vfs string) (dbId uint32, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return 0, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	if err := p.writer.WriteOpen(name, flags, vfs); err != nil {
		return 0, err
	}

	return p.reader.ReadDb()
}

func (p *Protocol) Prepare(ctx context.Context, db uint32, sql string) (id uint32, args uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return 0, 0, p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	if err := p.writer.WritePrepare(db, sql); err != nil {
		return 0, 0, err
	}

	return p.reader.ReadStmt()
}

func (p *Protocol) Finalize(ctx context.Context, dbId, stmtId uint32) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return p.netErr
	}

	defer func() {
		if err == nil {
			return
		}
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	if err := p.writer.WriteFinalize(dbId, stmtId); err != nil {
		return err
	}

	return p.reader.ReadAck()
}

// Close the client connection.
func (p *Protocol) Close() error {
	close(p.closeCh)
	return p.conn.Close()
}

func (p *Protocol) send(req *Message) error {
	if err := p.sendHeader(req); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := p.sendBody(req); err != nil {
		return errors.Wrap(err, "body")
	}

	return nil
}

func (p *Protocol) sendHeader(req *Message) error {
	n, err := p.conn.Write(req.header[:])
	if err != nil {
		return err
	}

	if n != messageHeaderSize {
		return io.ErrShortWrite
	}

	return nil
}

func (p *Protocol) sendBody(req *Message) error {
	buf := req.body.Bytes[:req.body.Offset]
	n, err := p.conn.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func (p *Protocol) recv(res *Message) error {
	res.reset()

	if err := p.recvHeader(res); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := p.recvBody(res); err != nil {
		return errors.Wrap(err, "body")
	}

	return nil
}

func (p *Protocol) recvHeader(res *Message) error {
	if err := p.recvPeek(res.header); err != nil {
		return err
	}

	res.words = binary.LittleEndian.Uint32(res.header[0:])
	res.mtype = res.header[4]
	res.schema = res.header[5]
	res.extra = binary.LittleEndian.Uint16(res.header[6:])

	return nil
}

func (p *Protocol) recvBody(res *Message) error {
	n := int(res.words) * messageWordSize

	for n > len(res.body.Bytes) {
		// Grow message buffer.
		bytes := make([]byte, len(res.body.Bytes)*2)
		res.body.Bytes = bytes
	}

	buf := res.body.Bytes[:n]

	if err := p.recvPeek(buf); err != nil {
		return err
	}

	return nil
}

// Read until buf is full.
func (p *Protocol) recvPeek(buf []byte) error {
	for offset := 0; offset < len(buf); {
		n, err := p.recvFill(buf[offset:])
		if err != nil {
			return err
		}
		offset += n
	}

	return nil
}

// Try to fill buf, but perform at most one read.
func (p *Protocol) recvFill(buf []byte) (int, error) {
	// Read new data: try a limited number of times.
	//
	// This technique is copied from bufio.Reader.
	for i := messageMaxConsecutiveEmptyReads; i > 0; i-- {
		n, err := p.conn.Read(buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if err != nil {
			return -1, err
		}
		if n > 0 {
			return n, nil
		}
	}
	return -1, io.ErrNoProgress
}

// DecodeNodeCompat handles also pre-1.0 legacy server messages.
func DecodeNodeCompat(protocol *Protocol, response *Message) (uint64, string, error) {
	if protocol.version == VersionLegacy {
		address, err := DecodeNodeLegacy(response)
		if err != nil {
			return 0, "", err
		}
		return 0, address, nil

	}
	return DecodeNode(response)
}
