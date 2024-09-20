package protocol

import (
	"context"
	"database/sql/driver"
	"fmt"
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

func (p *Protocol) Cluster(ctx context.Context) (nodes Nodes, err error) {
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

	if err := p.writer.WriteCluster(ClusterFormatV1); err != nil {
		return nil, err
	}

	return p.reader.ReadClusterV1()
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

func (p *Protocol) Transfer(ctx context.Context, id uint64) (err error) {
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

	if err := p.writer.WriteTransfer(id); err != nil {
		return err
	}

	return p.reader.ReadAck()
}

func (p *Protocol) Describe(ctx context.Context) (metadata *NodeMetadata, err error) {
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

	if err := p.writer.WriteDescribe(RequestDescribeFormatV0); err != nil {
		return nil, err
	}

	return p.reader.ReadMetadataV0()
}

func (p *Protocol) Assign(ctx context.Context, id uint64, role uint64) (err error) {
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

	if err := p.writer.WriteAssign(id, role); err != nil {
		return err
	}

	return p.reader.ReadAck()
}

func (p *Protocol) Remove(ctx context.Context, id uint64) (err error) {
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

	if err := p.writer.WriteRemove(id); err != nil {
		return err
	}

	return p.reader.ReadAck()
}

func (p *Protocol) Weight(ctx context.Context, weight uint64) (err error) {
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

	if err := p.writer.WriteWeight(weight); err != nil {
		return err
	}

	return p.reader.ReadAck()
}

func (p *Protocol) Add(ctx context.Context, id uint64, address string) (err error) {
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

	if err := p.writer.WriteAdd(id, address); err != nil {
		return err
	}

	return p.reader.ReadAck()
}

func (p *Protocol) Dump(ctx context.Context, name string) (files []File, err error) {
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

	if err := p.writer.WriteDump(name); err != nil {
		return nil, err
	}
	return p.reader.ReadFiles()
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
