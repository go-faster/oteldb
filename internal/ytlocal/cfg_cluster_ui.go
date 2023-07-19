package ytlocal

// Cluster config.
type Cluster struct {
	ID             string               `json:"id"`
	Name           string               `json:"name,omitempty"`
	Proxy          string               `json:"proxy,omitempty"`
	Secure         bool                 `json:"secure"`
	Theme          string               `json:"theme,omitempty"`
	Authentication string               `json:"authentication"`
	Group          string               `json:"group,omitempty"`
	Environment    string               `json:"environment,omitempty"`
	Description    string               `json:"description,omitempty"`
	PrimaryMaster  ClusterPrimaryMaster `json:"primaryMaster"`
}

// ClusterPrimaryMaster config.
type ClusterPrimaryMaster struct {
	CellTag int `json:"cellTag"`
}

// ClusterConfig config.
type ClusterConfig struct {
	Clusters []Cluster `json:"clusters"`
}
