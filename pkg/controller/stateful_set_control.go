package controller

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	apps "k8s.io/api/apps/v1"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
)

type StatefulSetControlInterface interface {
	CreateStatefulSet(*v1alpha1.HdfsCluster, *apps.StatefulSet) error
	GetStatefulSet(hc *v1alpha1.HdfsCluster, name string) (*apps.StatefulSet, error)
	UpdateStatefulSet(*v1alpha1.HdfsCluster, *apps.StatefulSet) (*apps.StatefulSet, error)
}

type realStatefulSetControl struct {
	kubeCli    kubernetes.Interface
	setListers appslisters.StatefulSetLister
}

// NewRealServiceControl creates a new ServiceControlInterface
func NewRealStatefulSetControl(kubeCli kubernetes.Interface, setListers appslisters.StatefulSetLister) StatefulSetControlInterface {
	return &realStatefulSetControl{
		kubeCli,
		setListers,
	}
}

func (c *realStatefulSetControl) CreateStatefulSet(hc *v1alpha1.HdfsCluster, statefulSet *apps.StatefulSet) error {
	_, err := c.kubeCli.AppsV1().StatefulSets(hc.Namespace).Create(statefulSet)
	if err != nil {
		glog.Errorf("create StatefulSet(error, err=%+v", err)
		return err
	}
	return nil
}

func (c *realStatefulSetControl) GetStatefulSet(hc *v1alpha1.HdfsCluster, name string) (*apps.StatefulSet, error) {
	set, err := c.setListers.StatefulSets(hc.Namespace).Get(name)
	return set, err
}

func (c *realStatefulSetControl) UpdateStatefulSet(hc *v1alpha1.HdfsCluster, ss *apps.StatefulSet) (*apps.StatefulSet, error) {
	cur, err := c.kubeCli.AppsV1().StatefulSets(hc.Namespace).Update(ss)
	if err != nil {
		glog.Errorf("update StatefulSet error, err=%+v", err)
		return nil, err
	}
	return cur, nil
}
