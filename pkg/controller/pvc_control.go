package controller

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type PVCControlInterface interface {
	CreatePVC(*v1alpha1.HdfsCluster, *corev1.PersistentVolumeClaim) error
}

type realPVCControl struct {
	kubeCli kubernetes.Interface
}

func NewRealPVCControl(kubeCli kubernetes.Interface) PVCControlInterface {
	return &realPVCControl{
		kubeCli: kubeCli,
	}
}

func (c *realPVCControl) CreatePVC(hc *v1alpha1.HdfsCluster, pvc *corev1.PersistentVolumeClaim) error {
	_, err := c.kubeCli.CoreV1().PersistentVolumeClaims(hc.Namespace).Create(pvc)
	if err != nil {
		glog.Errorf("create pvc error, err=%+v", err)
		return err
	}
	return nil
}
