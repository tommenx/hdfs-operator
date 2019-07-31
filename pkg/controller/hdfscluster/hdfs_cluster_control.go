package hdfscluster

import (
	"errors"
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/manager"
)

type ControlInterface interface {
	UpdateHdfsCluster(cluster *v1alpha1.HdfsCluster) error
}

type hdfsClusterControl struct {
	nameNodeManager manager.Manager
	dataNodeManager manager.Manager
}

func NewHdfsClusterControl(
	nameNodeManager manager.Manager,
	dataNodeManager manager.Manager,
) ControlInterface {
	return &hdfsClusterControl{
		nameNodeManager: nameNodeManager,
		dataNodeManager: dataNodeManager,
	}
}
func (c *hdfsClusterControl) UpdateHdfsCluster(cluster *v1alpha1.HdfsCluster) error {
	err := c.updateHdfsCluster(cluster)
	if err != nil {
		glog.Errorf("update hdfs cluster failed, err=%+v", err)
		return err
	}
	return nil
}

//同步name node的部署配置
//检查name node的服务是否可用
//同步data node的部署配置
func (c *hdfsClusterControl) updateHdfsCluster(cluster *v1alpha1.HdfsCluster) error {
	if err := c.nameNodeManager.Sync(cluster); err != nil {
		glog.Errorf("sync name node error")
		return err
	}
	if !c.isNameNodeAvailable() {
		glog.Error("name node service is not available")
		return errors.New("name node is not available")
	}
	return nil
}

//检查name node是否已经能够运行
//通过检查name node pod 的状态，
func (c *hdfsClusterControl) isNameNodeAvailable() bool {
	ok := c.nameNodeManager.CheckStatus()
	if !ok {
		return false
	}
}
