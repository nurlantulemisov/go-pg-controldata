package controldata

import (
	"errors"
	"os"
	"path/filepath"
	"unsafe"
)

type DBState int

const (
	DBStartup DBState = iota
	DBShutdowned
	DBShutdownedInRecovery
	DBShutdowning
	DBInCrashRecovery
	DBInArchiveRecovery
	DBInProduction
)

func (s DBState) String() string {
	switch s {
	case DBStartup:
		return "starting up"
	case DBShutdowned:
		return "shut down"
	case DBShutdownedInRecovery:
		return "shut down in recovery"
	case DBShutdowning:
		return "shutting down"
	case DBInCrashRecovery:
		return "in crash recovery"
	case DBInArchiveRecovery:
		return "in archive recovery"
	case DBInProduction:
		return "in production"
	default:
		return "unrecognized status code"
	}
}

type XLogRecPtr uint64
type TimelineID uint32
type FullTransactionID uint64
type OID uint32
type TransactionID uint32
type MultiXactID TransactionID
type MultiXactOffset uint32

type CheckPoint struct {
	Redo              XLogRecPtr        // next RecPtr available at creation
	ThisTimeLineID    TimelineID        // current TLI
	PrevTimeLineID    TimelineID        // previous TLI or equal to ThisTimeLineID
	FullPageWrites    bool              // current full_page_writes
	NextXid           FullTransactionID // next free transaction ID
	NextOid           OID               // next free OID
	NextMulti         MultiXactID       // next free MultiXactId
	NextMultiOffset   MultiXactOffset   // next free MultiXact offset
	OldestXid         TransactionID     // cluster-wide minimum datfrozenxid
	OldestXidDB       OID               // database with minimum datfrozenxid
	OldestMulti       MultiXactID       // cluster-wide minimum datminmxid
	OldestMultiDB     OID               // database with minimum datminmxid
	Time              int64             // time stamp of checkpoint
	OldestCommitTsXid TransactionID     // oldest Xid with valid commit timestamp
	NewestCommitTsXid TransactionID     // newest Xid with valid commit timestamp

	// Only used in hot standby initialization from an online checkpoint
	OldestActiveXid TransactionID // oldest XID still running
}

type ControlFileData struct {
	SystemID             uint64
	PgControlVersion     uint32
	CatalogVersionNo     uint32
	State                DBState
	Time                 int64
	CheckPoint           XLogRecPtr
	CheckPointCopy       CheckPoint
	UnloggedLSN          XLogRecPtr
	MinRecoveryPoint     XLogRecPtr
	MinRecoveryPointTLI  TimelineID
	BackupStartPoint     XLogRecPtr
	BackupEndPoint       XLogRecPtr
	BackupEndRequired    bool
	WalLevel             int
	WalLogHints          bool
	MaxConnections       int
	MaxWorkerProcesses   int
	MaxWalSenders        int
	MaxPreparedXacts     int
	MaxLocksPerXact      int
	TrackCommitTimestamp bool
	MaxAlign             uint32
	FloatFormat          float64
	Blcksz               uint32
	RelsegSize           uint32
	XlogBlcksz           uint32
	XlogSegSize          uint32
	NameDataLen          uint32
	IndexMaxKeys         uint32
	ToastMaxChunkSize    uint32
	Loblksize            uint32
	Float4ByVal          bool
	Float8ByVal          bool
	DataChecksumVersion  uint32
}

func Get(pgdata string) (*ControlFileData, error) {
	pgControl, err := os.Open(filepath.Join(pgdata, "global", "pg_control"))
	if err != nil {
		return nil, err
	}
	defer pgControl.Close()

	var controlfile ControlFileData
	if _, err = pgControl.Read((*[unsafe.Sizeof(controlfile)]byte)(unsafe.Pointer(&controlfile))[:]); err != nil {
		return nil, err
	}

	/* Make sure the control file is valid byte order. */
	if controlfile.PgControlVersion%65536 == 0 && controlfile.PgControlVersion/65536 != 0 {
		return nil, errors.New("not valid byte order")
	}

	return &controlfile, nil
}
