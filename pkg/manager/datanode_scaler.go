package manager

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	apps "k8s.io/api/apps/v1"
)

//TODO
//manage pvc
type dataNodeScaler struct {
}

func ScaleOut(hc v1alpha1.HdfsCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	ns := hc.GetNamespace()
	tcName := hc.GetName()
	glog.Infof("start scale %s/%s", ns, tcName)
	//TODO
	//should add some HDFS state check
	//like check live datanodes, datanodes status to decide if it can scale out
	increaseReplicas(newSet, oldSet)
	return nil
}
