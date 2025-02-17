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
	PermitShared          bool
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

	protocol, err := protocol.Handshake(ctx, conn, protocol.VersionOne, address)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{protocol}, nil
}

// Leader returns information about the current leader, if any.
func (c *Client) Leader(ctx context.Context) (*NodeInfo, error) {
	request := protocol.Message{}
	request.Init(16)
	response := protocol.Message{}
	response.Init(512)

	protocol.EncodeLeader(&request)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return nil, errors.Wrap(err, "failed to send Leader request")
	}

	id, address, err := protocol.DecodeNode(&response)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Node response")
	}

	info := &NodeInfo{ID: id, Address: address}

	return info, nil
}

// Cluster returns information about all nodes in the cluster.
func (c *Client) Cluster(ctx context.Context) ([]NodeInfo, error) {
	request := protocol.Message{}
	request.Init(16)
	response := protocol.Message{}
	response.Init(512)

	protocol.EncodeCluster(&request, protocol.ClusterFormatV1)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return nil, errors.Wrap(err, "failed to send Cluster request")
	}

	servers, err := protocol.DecodeNodes(&response)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Node response")
	}

	return servers, nil
}

// File holds the content of a single database file.
type File struct {
	Name string
	Data []byte
}

// Dump the content of the database with the given name. Two files will be
// returned, the first is the main database file (which has the same name as
// the database), the second is the WAL file (which has the same name as the
// database plus the suffix "-wal").
func (c *Client) Dump(ctx context.Context, dbname string) ([]File, error) {
	request := protocol.Message{}
	request.Init(16)
	response := protocol.Message{}
	response.Init(512)

	protocol.EncodeDump(&request, dbname)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return nil, errors.Wrap(err, "failed to send dump request")
	}

	files, err := protocol.DecodeFiles(&response)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse files response")
	}
	defer files.Close()

	dump := make([]File, 0)

	for {
		name, data := files.Next()
		if name == "" {
			break
		}
		dump = append(dump, File{Name: name, Data: data})
	}

	return dump, nil
}

// Add a node to a cluster.
//
// The new node will have the role specified in node.Role. Note that if the
// desired role is Voter, the node being added must be online, since it will be
// granted voting rights only once it catches up with the leader's log.
func (c *Client) Add(ctx context.Context, node NodeInfo) error {
	request := protocol.Message{}
	response := protocol.Message{}

	request.Init(4096)
	response.Init(4096)

	protocol.EncodeAdd(&request, node.ID, node.Address)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return err
	}

	if err := protocol.DecodeEmpty(&response); err != nil {
		return err
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
	request := protocol.Message{}
	response := protocol.Message{}

	request.Init(4096)
	response.Init(4096)

	protocol.EncodeAssign(&request, id, uint64(role))

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return err
	}

	if err := protocol.DecodeEmpty(&response); err != nil {
		return err
	}

	return nil
}

// Transfer leadership from the current leader to another node.
//
// This must be invoked one client connected to the current leader.
func (c *Client) Transfer(ctx context.Context, id uint64) error {
	request := protocol.Message{}
	response := protocol.Message{}

	request.Init(4096)
	response.Init(4096)

	protocol.EncodeTransfer(&request, id)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return err
	}

	if err := protocol.DecodeEmpty(&response); err != nil {
		return err
	}

	return nil
}

// Remove a node from the cluster.
func (c *Client) Remove(ctx context.Context, id uint64) error {
	request := protocol.Message{}
	request.Init(4096)
	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeRemove(&request, id)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return err
	}

	if err := protocol.DecodeEmpty(&response); err != nil {
		return err
	}

	return nil
}

// NodeMetadata user-defined node-level metadata.
type NodeMetadata struct {
	FailureDomain uint64
	Weight        uint64
}

// Describe returns metadata about the node we're connected with.
func (c *Client) Describe(ctx context.Context) (*NodeMetadata, error) {
	request := protocol.Message{}
	request.Init(4096)
	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeDescribe(&request, protocol.RequestDescribeFormatV0)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return nil, err
	}

	domain, weight, err := protocol.DecodeMetadata(&response)
	if err != nil {
		return nil, err
	}

	metadata := &NodeMetadata{
		FailureDomain: domain,
		Weight:        weight,
	}

	return metadata, nil
}

// Weight updates the weight associated to the node we're connected with.
func (c *Client) Weight(ctx context.Context, weight uint64) error {
	request := protocol.Message{}
	request.Init(4096)
	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeWeight(&request, weight)

	if err := c.protocol.Call(ctx, &request, &response); err != nil {
		return err
	}

	if err := protocol.DecodeEmpty(&response); err != nil {
		return err
	}

	return nil
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

// Connector is a reusable configuration for creating new Clients.
//
// In some cases, Connector.Connect can take advantage of state stored in the
// Connector to be more efficient than New or FindLeader, so prefer to use a
// Connector whenever several Clients need to be created with the same
// parameters.
type Connector protocol.Connector

// NewLeaderConnector creates a Connector that will yield Clients connected to
// the cluster leader.
func NewLeaderConnector(store NodeStore, options ...Option) *Connector {
	opts := defaultOptions()
	for _, o := range options {
		o(opts)
	}
	config := protocol.Config{
		Dial:                  opts.DialFunc,
		ConcurrentLeaderConns: opts.ConcurrentLeaderConns,
		PermitShared:          opts.PermitShared,
	}
	inner := protocol.NewLeaderConnector(store, config, opts.LogFunc)
	return (*Connector)(inner)
}

// NewDirectConnector creates a Connector that will yield Clients connected to
// the node with the given ID and address.
func NewDirectConnector(id uint64, address string, options ...Option) *Connector {
	opts := defaultOptions()
	for _, o := range options {
		o(opts)
	}
	config := protocol.Config{Dial: opts.DialFunc}
	inner := protocol.NewDirectConnector(id, address, config, opts.LogFunc)
	return (*Connector)(inner)
}

// Connect opens a Client based on the Connector's configuration.
func (connector *Connector) Connect(ctx context.Context) (*Client, error) {
	protocol, err := (*protocol.Connector)(connector).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &Client{protocol}, nil
}
