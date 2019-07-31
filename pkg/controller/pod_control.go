package controller

import (
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type PodControlInterface interface {
	CheckPodsStatus(apps map[string]string) (bool, map[string]string, error)
}

type realPodControl struct {
	kubeCli   kubernetes.Interface
	podLister corelisters.PodLister
}

func NewRealPodControl(kubeCli kubernetes.Interface, podLister corelisters.PodLister) PodControlInterface {
	return &realPodControl{
		kubeCli:   kubeCli,
		podLister: podLister,
	}
}

func (c *realPodControl) CheckPodsStatus(apps map[string]string) (bool, map[string]string, error) {
	sel := labels.SelectorFromSet(apps)
	pods, err := c.podLister.List(sel)
	if err != nil {
		glog.Errorf("List pods error, err=%+v", err)
		return false, nil, err
	}
	status := make(map[string]string)
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning {
			status[pod.Name] = string(pod.Status.Phase)
		}
	}
	if len(status) != 0 {
		glog.Errorf("%d pods are not running", len(status))
		return false, status, nil
	}
	return true, nil, nil
}
