package controller

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type ServiceControlInterface interface {
	CreateService(*v1alpha1.HdfsCluster, *corev1.Service) error
}

type realServiceControl struct {
	kubeCli kubernetes.Interface
}

// NewRealServiceControl creates a new ServiceControlInterface
func NewRealServiceControl(kubeCli kubernetes.Interface) ServiceControlInterface {
	return &realServiceControl{
		kubeCli,
	}
}

func (c *realServiceControl) CreateService(hc *v1alpha1.HdfsCluster, svc *corev1.Service) error {
	_, err := c.kubeCli.CoreV1().Services(hc.Namespace).Create(svc)
	if err != nil {
		glog.Errorf("create service error, err=%+v", err)
		return err
	}
	return nil
}
