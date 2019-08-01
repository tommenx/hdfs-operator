package controller

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/client/clientset/versioned"
	imformers "github.com/tommenx/hdfs-operator/pkg/client/informers/externalversions"
	"k8s.io/client-go/tools/clientcmd"
	"time"
)

var (
	resyncDuration = time.Second * 30
)

func NewSLCliAndInformerFactory(path string) (versioned.Interface, imformers.SharedInformerFactory) {
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		glog.Errorf("create kubernetes config error, err=%+v", err)
		panic(err)
	}
	cli, err := versioned.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("failed to create Clientset: %v", err)
	}
	informerFactory := imformers.NewSharedInformerFactory(cli, resyncDuration)
	return cli, informerFactory
}
