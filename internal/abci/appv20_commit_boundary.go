package abci

// AppV20CommitBoundaryStage names the two cross-store crash windows pinned by
// the v11.9 fault fixtures. Production builds implement the hook as a no-op.
type AppV20CommitBoundaryStage string

const (
	// AppV20CommitAfterOffchainFlush fires after the SQLite/Postgres transaction
	// commits successfully and immediately before SaveState/Badger commit.
	AppV20CommitAfterOffchainFlush AppV20CommitBoundaryStage = "after_offchain_flush"
	// AppV20CommitAfterBadgerSync fires after the whole-block Badger transaction
	// commits and fsyncs, immediately before the speculative app graph is published.
	AppV20CommitAfterBadgerSync AppV20CommitBoundaryStage = "after_badger_sync"
)
