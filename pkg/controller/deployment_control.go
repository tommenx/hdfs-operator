package controller

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	apps "k8s.io/api/apps/v1"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
)

type DeploymentControlInterface interface {
	CreateDeployment(*v1alpha1.HdfsCluster, *apps.Deployment) error
	GetDeployment(hc *v1alpha1.HdfsCluster, deployment string) (*apps.Deployment, error)
}

type realDeploymentControl struct {
	kubeCli      kubernetes.Interface
	deployLister appslisters.DeploymentLister
}

// NewRealServiceControl creates a new ServiceControlInterface
func NewRealDeploymentControl(kubeCli kubernetes.Interface, deployListers appslisters.DeploymentLister) DeploymentControlInterface {
	return &realDeploymentControl{
		kubeCli,
		deployListers,
	}
}

func (c *realDeploymentControl) CreateDeployment(hc *v1alpha1.HdfsCluster, deployment *apps.Deployment) error {
	_, err := c.kubeCli.AppsV1().Deployments(hc.Namespace).Create(deployment)
	if err != nil {
		glog.Errorf("create deployment error, err=%+v", err)
		return err
	}
	return nil
}

func (c *realDeploymentControl) GetDeployment(hc *v1alpha1.HdfsCluster, deployment string) (*apps.Deployment, error) {
	cur, err := c.deployLister.Deployments(hc.Namespace).Get(deployment)
	return cur, err
}
