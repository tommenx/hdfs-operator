package manager

import (
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
)

type Manager interface {
	Sync(cluster *v1alpha1.HdfsCluster) error
	CheckStatus() bool
}
