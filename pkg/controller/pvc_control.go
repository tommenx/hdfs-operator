package controller

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type PVCControlInterface interface {
	CreatePVC(*v1alpha1.HdfsCluster, *corev1.PersistentVolumeClaim) error
	GetPVC(hc *v1alpha1.HdfsCluster, name string) (*corev1.PersistentVolumeClaim, error)
}

type realPVCControl struct {
	kubeCli   kubernetes.Interface
	pvcLister corelisters.PersistentVolumeClaimLister
}

func NewRealPVCControl(kubeCli kubernetes.Interface, pvcLister corelisters.PersistentVolumeClaimLister) PVCControlInterface {
	return &realPVCControl{
		kubeCli:   kubeCli,
		pvcLister: pvcLister,
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

func (c *realPVCControl) GetPVC(hc *v1alpha1.HdfsCluster, name string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := c.pvcLister.PersistentVolumeClaims(hc.Namespace).Get(name)
	return pvc, err

}
