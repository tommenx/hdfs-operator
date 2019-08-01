package main

import (
	"flag"
	"github.com/tommenx/hdfs-operator/pkg/controller"
	"github.com/tommenx/hdfs-operator/pkg/controller/hdfscluster"
)

func init() {
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()
	path := "/root/.kube/config"
	kubeCli, kubeInformerFactory := controller.NewCliAndInformer(path)
	cli, informerFactory := controller.NewSLCliAndInformerFactory(path)
	stopCh := make(chan struct{})
	defer close(stopCh)
	control := hdfscluster.NewController(kubeCli, cli, informerFactory, kubeInformerFactory)
	go informerFactory.Start(stopCh)
	go kubeInformerFactory.Start(stopCh)
	control.Run(1, stopCh)

}
