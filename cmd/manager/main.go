package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/controller"
	"github.com/tommenx/hdfs-operator/pkg/controller/hdfscluster"
	"github.com/tommenx/hdfs-operator/pkg/manager"
	"k8s.io/client-go/tools/cache"
)

func init() {
	flag.Set("logtostderr", "true")
}
func main() {
	flag.Parse()
	path := "/root/.kube/config"
	kubeCli, informerFactory := controller.NewCliAndInformer(path)
	cli, _ := controller.NewSLCliAndInformerFactory(path)
	stopCh := make(chan struct{})
	podInformer := informerFactory.Core().V1().Pods()
	svcInformer := informerFactory.Core().V1().Services()
	deployInformer := informerFactory.Apps().V1().Deployments()
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	go informerFactory.Start(stopCh)
	svcControl := controller.NewRealServiceControl(kubeCli, svcInformer.Lister())
	pvcControl := controller.NewRealPVCControl(kubeCli, pvcInformer.Lister())
	deployControl := controller.NewRealDeploymentControl(kubeCli, deployInformer.Lister())
	podControl := controller.NewRealPodControl(kubeCli, podInformer.Lister())
	if !cache.WaitForCacheSync(stopCh, podInformer.Informer().HasSynced, svcInformer.Informer().HasSynced) {
		return
	}
	hdfsControl := hdfscluster.NewHdfsController(cli)
	namenode := manager.NewNameNodeManager(deployControl, pvcControl, podControl, svcControl)
	hc, err := hdfsControl.Get()
	if err != nil {
		glog.Errorf("get hdfs cluster error,err=%+v", err)
		panic(err)
	}
	err = namenode.Sync(hc)
	if err != nil {
		glog.Errorf("sync name node error")
	}
}
