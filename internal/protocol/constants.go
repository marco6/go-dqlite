package protocol

// VersionOne is version 1 of the server protocol.
const VersionOne = uint64(1)

// VersionLegacy is the pre 1.0 dqlite server protocol version.
const VersionLegacy = uint64(0x86104dd760433fe5)

// Cluster response formats
type ClusterFormat uint64

const (
	ClusterFormatV0 ClusterFormat = 0
	ClusterFormatV1 ClusterFormat = 1
)

type ColumnType uint8

const (
	// SQLite datatype codes
	Integer ColumnType = 1
	Float   ColumnType = 2
	Text    ColumnType = 3
	Blob    ColumnType = 4
	Null    ColumnType = 5

	// Special data types for time values.
	UnixTime ColumnType = 9
	ISO8601  ColumnType = 10
	Boolean  ColumnType = 11
)

func (t ColumnType) String() string {
	switch t {
	case Integer:
		return "INTEGER"
	case Float:
		return "FLOAT"
	case Blob:
		return "BLOB"
	case Text:
		return "TEXT"
	case Null:
		return "NULL"
	case UnixTime:
		return "TIME"
	case ISO8601:
		return "TIME"
	case Boolean:
		return "BOOL"
	default:
		return ""
	}
}

type RequestType uint8

// Request types.
const (
	RequestLeader    RequestType = 0
	RequestClient    RequestType = 1
	RequestHeartbeat RequestType = 2
	RequestOpen      RequestType = 3
	RequestPrepare   RequestType = 4
	RequestExec      RequestType = 5
	RequestQuery     RequestType = 6
	RequestFinalize  RequestType = 7
	RequestExecSQL   RequestType = 8
	RequestQuerySQL  RequestType = 9
	RequestInterrupt RequestType = 10
	RequestAdd       RequestType = 12
	RequestAssign    RequestType = 13
	RequestRemove    RequestType = 14
	RequestDump      RequestType = 15
	RequestCluster   RequestType = 16
	RequestTransfer  RequestType = 17
	RequestDescribe  RequestType = 18
	RequestWeight    RequestType = 19
)

func (request RequestType) String() string {
	switch request {
	case RequestLeader:
		return "leader"
	case RequestClient:
		return "client"
	case RequestHeartbeat:
		return "heartbeat"
	case RequestOpen:
		return "open"
	case RequestPrepare:
		return "prepare"
	case RequestExec:
		return "exec"
	case RequestQuery:
		return "query"
	case RequestFinalize:
		return "finalize"
	case RequestExecSQL:
		return "exec-sql"
	case RequestQuerySQL:
		return "query-sql"
	case RequestInterrupt:
		return "interrupt"
	case RequestAdd:
		return "add"
	case RequestAssign:
		return "assign"
	case RequestRemove:
		return "remove"
	case RequestDump:
		return "dump"
	case RequestCluster:
		return "cluster"
	case RequestTransfer:
		return "transfer"
	case RequestDescribe:
		return "describe"
	default:
		return "unknown"
	}
}

type DescribeFormat uint64

// Formats
const (
	RequestDescribeFormatV0 DescribeFormat = 0
)

type ResponseType uint8

// Response types.
const (
	ResponseFailure    ResponseType = 0
	ResponseNode       ResponseType = 1
	ResponseNodeLegacy ResponseType = 1
	ResponseWelcome    ResponseType = 2
	ResponseNodes      ResponseType = 3
	ResponseDb         ResponseType = 4
	ResponseStmt       ResponseType = 5
	ResponseResult     ResponseType = 6
	ResponseRows       ResponseType = 7
	ResponseEmpty      ResponseType = 8
	ResponseFiles      ResponseType = 9
	ResponseMetadata   ResponseType = 10
)

// Human-readable description of a response type.
func (response ResponseType) String() string {
	switch response {
	case ResponseFailure:
		return "failure"
	case ResponseNode:
		return "node"
	case ResponseWelcome:
		return "welcome"
	case ResponseNodes:
		return "nodes"
	case ResponseDb:
		return "db"
	case ResponseStmt:
		return "stmt"
	case ResponseResult:
		return "result"
	case ResponseRows:
		return "rows"
	case ResponseEmpty:
		return "empty"
	case ResponseFiles:
		return "files"
	case ResponseMetadata:
		return "metadata"
	default:
		return "unknown"
	}
}
