package manager

import "github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"

type dataNodeManager struct {
}

func NewDataNodeManager() Manager {
	return &dataNodeManager{}
}

func (m *dataNodeManager) Sync(cluster *v1alpha1.HdfsCluster) error {
	return nil
}
func (m *dataNodeManager) CheckStatus() bool {
	return false
}
