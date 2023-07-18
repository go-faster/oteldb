package ytlocal

// QueryTracker config.
type QueryTracker struct {
	BaseServer
	User                       string `yson:"user"`
	CreateStateTablesOnStartup bool   `yson:"create_state_tables_on_startup"`
}
