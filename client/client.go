package client

import (
	"context"

	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/pkg/errors"
)

// DialFunc is a function that can be used to establish a network connection.
type DialFunc = protocol.DialFunc

// Client speaks the dqlite wire protocol.
type Client struct {
	protocol *protocol.Protocol
}

// Option that can be used to tweak client parameters.
type Option func(*options)

type options struct {
	DialFunc              DialFunc
	LogFunc               LogFunc
	ConcurrentLeaderConns int64
}

// WithDialFunc sets a custom dial function for creating the client network
// connection.
func WithDialFunc(dial DialFunc) Option {
	return func(options *options) {
		options.DialFunc = dial
	}
}

// WithLogFunc sets a custom log function.
// connection.
func WithLogFunc(log LogFunc) Option {
	return func(options *options) {
		options.LogFunc = log
	}
}

// WithConcurrentLeaderConns is the maximum number of concurrent connections
// to other cluster members that will be attempted while searching for the dqlite leader.
//
// The default is 10 connections to other cluster members.
func WithConcurrentLeaderConns(maxConns int64) Option {
	return func(o *options) {
		o.ConcurrentLeaderConns = maxConns
	}
}

// New creates a new client connected to the dqlite node with the given
// address.
func New(ctx context.Context, address string, options ...Option) (*Client, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}
	// Establish the connection.
	conn, err := o.DialFunc(ctx, address)
	if err != nil {
		return nil, errors.Wrap(err, "failed to establish network connection")
	}

	protocol, err := protocol.NewProtocol(conn, protocol.VersionOne)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{protocol: protocol}, nil
}

// Leader returns information about the current leader, if any.
func (c *Client) Leader(ctx context.Context) (*NodeInfo, error) {
	id, address, err := c.protocol.Leader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Node response")
	}

	return &NodeInfo{
		ID:      id,
		Address: address,
	}, nil
}

// Cluster returns information about all nodes in the cluster.
func (c *Client) Cluster(ctx context.Context) ([]NodeInfo, error) {
	nodes, err := c.protocol.Cluster(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send Cluster request")
	}
	return nodes, nil
}

// File holds the content of a single database file.
type File = protocol.File

// Dump the content of the database with the given name. Two files will be
// returned, the first is the main database file (which has the same name as
// the database), the second is the WAL file (which has the same name as the
// database plus the suffix "-wal").
func (c *Client) Dump(ctx context.Context, dbname string) ([]File, error) {
	files, err := c.protocol.Dump(ctx, dbname)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send dump request")
	}
	return files, nil
}

// Add a node to a cluster.
//
// The new node will have the role specified in node.Role. Note that if the
// desired role is Voter, the node being added must be online, since it will be
// granted voting rights only once it catches up with the leader's log.
func (c *Client) Add(ctx context.Context, node NodeInfo) error {
	err := c.protocol.Add(ctx, node.ID, node.Address)
	if err != nil {
		return nil
	}

	// If the desired role is spare, there's nothing to do, since all newly
	// added nodes have the spare role.
	if node.Role == Spare {
		return nil
	}

	return c.Assign(ctx, node.ID, node.Role)
}

// Assign a role to a node.
//
// Possible roles are:
//
// - Voter: the node will replicate data and participate in quorum.
// - StandBy: the node will replicate data but won't participate in quorum.
// - Spare: the node won't replicate data and won't participate in quorum.
//
// If the target node does not exist or has already the desired role, an error
// is returned.
func (c *Client) Assign(ctx context.Context, id uint64, role NodeRole) error {
	return c.protocol.Assign(ctx, id, uint64(role))
}

// Transfer leadership from the current leader to another node.
//
// This must be invoked one client connected to the current leader.
func (c *Client) Transfer(ctx context.Context, id uint64) error {
	return c.protocol.Transfer(ctx, id)
}

// Remove a node from the cluster.
func (c *Client) Remove(ctx context.Context, id uint64) error {
	return c.protocol.Remove(ctx, id)
}

// NodeMetadata user-defined node-level metadata.
type NodeMetadata = protocol.NodeMetadata

// Describe returns metadata about the node we're connected with.
func (c *Client) Describe(ctx context.Context) (*NodeMetadata, error) {
	metadata, err := c.protocol.Describe(ctx)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

// Weight updates the weight associated to the node we're connected with.
func (c *Client) Weight(ctx context.Context, weight uint64) error {
	return c.protocol.Weight(ctx, weight)
}

// Close the client.
func (c *Client) Close() error {
	return c.protocol.Close()
}

// Create a client options object with sane defaults.
func defaultOptions() *options {
	return &options{
		DialFunc:              DefaultDialFunc,
		LogFunc:               DefaultLogFunc,
		ConcurrentLeaderConns: protocol.MaxConcurrentLeaderConns,
	}
}
