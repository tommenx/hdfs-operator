package manager

import (
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	apps "k8s.io/api/apps/v1"
)

type Scaler interface {
	ScaleOut(cluster v1alpha1.HdfsCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error
}

func increaseReplicas(newSet *apps.StatefulSet, oldSet *apps.StatefulSet) {
	*newSet.Spec.Replicas = *oldSet.Spec.Replicas + 1
}
