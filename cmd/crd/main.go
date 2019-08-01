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
	cli, _ := controller.NewSLCliAndInformerFactory(path)
	hdfs := hdfscluster.NewHdfsController(cli)
	hdfs.Get()
}
