package hdfscluster

import (
	"github.com/golang/glog"
	v1alpha1 "github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/client/clientset/versioned"
	informers "github.com/tommenx/hdfs-operator/pkg/client/informers/externalversions"
	listers "github.com/tommenx/hdfs-operator/pkg/client/listers/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/controller"
	"github.com/tommenx/hdfs-operator/pkg/manager"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	applisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	kubeClient      kubernetes.Interface
	cli             versioned.Interface
	hcLister        listers.HdfsClusterLister
	hcListerSynced  cache.InformerSynced
	setLister       applisters.StatefulSetLister
	setListerSynced cache.InformerSynced
	queue           workqueue.RateLimitingInterface
	control         ControlInterface
}

type HdfsController struct {
	cli versioned.Interface
}

func NewHdfsController(cli versioned.Interface) *HdfsController {
	return &HdfsController{cli: cli}
}

func (h *HdfsController) Get() (*v1alpha1.HdfsCluster, error) {
	hdfs, err := h.cli.StorageV1alpha1().HdfsClusters("default").Get("demo", metav1.GetOptions{})
	if err != nil {
		glog.Errorf("get hdfs cluster error")
		return nil, err
	}
	return hdfs, nil
}

func NewController(
	kubeCli kubernetes.Interface,
	cli versioned.Interface,
	informerFactory informers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
) *Controller {
	podInformer := kubeInformerFactory.Core().V1().Pods()
	hcInformer := informerFactory.Storage().V1alpha1().HdfsClusters()
	setInformer := kubeInformerFactory.Apps().V1().StatefulSets()

	setControl := controller.NewRealStatefulSetControl(kubeCli)
	svcControl := controller.NewRealServiceControl(kubeCli)
	pvcControl := controller.NewRealPVCControl(kubeCli)
	deployControl := controller.NewRealDeploymentControl(kubeCli)
	podControl := controller.NewRealPodControl(kubeCli, podInformer.Lister())

	control := &Controller{
		kubeClient: kubeCli,
		control: NewHdfsClusterControl(
			manager.NewNameNodeManager(deployControl, pvcControl, podControl, svcControl),
			manager.NewDataNodeManager(setControl, svcControl, manager.NewDataNodeScaler()),
		),
		queue: workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}
	hcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: control.enqueueHdfsCluster,
		UpdateFunc: func(old, cur interface{}) {
			control.enqueueHdfsCluster(cur)
		},
		DeleteFunc: control.enqueueHdfsCluster,
	})
	control.hcLister = hcInformer.Lister()
	control.hcListerSynced = hcInformer.Informer().HasSynced

	control.setLister = setInformer.Lister()
	control.setListerSynced = setInformer.Informer().HasSynced
	return control
}

func (c *Controller) enqueueHdfsCluster(obj interface{}) {

}
