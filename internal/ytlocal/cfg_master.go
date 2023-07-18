package ytlocal

// MasterChangelogs config.
type MasterChangelogs struct {
	Path string `yson:"path"`
}

// MasterSnapshots config.
type MasterSnapshots struct {
	Path string `yson:"path"`
}

// HydraManager config.
type HydraManager struct {
	MaxChangelogCountToKeep int `yson:"max_changelog_count_to_keep"`
	MaxSnapshotCountToKeep  int `yson:"max_snapshot_count_to_keep"`
}

// CypressManager config.
type CypressManager struct {
	DefaultTableReplicationFactor   int `yson:"default_table_replication_factor,omitempty"`
	DefaultFileReplicationFactor    int `yson:"default_file_replication_factor,omitempty"`
	DefaultJournalReplicationFactor int `yson:"default_journal_replication_factor,omitempty"`
	DefaultJournalReadQuorum        int `yson:"default_journal_read_quorum,omitempty"`
	DefaultJournalWriteQuorum       int `yson:"default_journal_write_quorum,omitempty"`
}

// Master config.
type Master struct {
	BaseServer
	PrimaryMaster    Connection       `yson:"primary_master"`
	UseNewHydra      bool             `yson:"use_new_hydra"`
	Changelogs       MasterChangelogs `yson:"snapshots"`
	Snapshots        MasterSnapshots  `yson:"changelogs"`
	HydraManager     HydraManager     `yson:"hydra_manager"`
	CypressManager   CypressManager   `yson:"cypress_manager"`
	SecondaryMasters []Connection     `yson:"secondary_masters"`
}
