package hdfscluster

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/client/clientset/versioned"
	informers "github.com/tommenx/hdfs-operator/pkg/client/informers/externalversions"
	listers "github.com/tommenx/hdfs-operator/pkg/client/listers/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/controller"
	"github.com/tommenx/hdfs-operator/pkg/manager"
	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	applisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"time"
)

var controllerKind = v1alpha1.SchemeGroupVersion.WithKind("HdfsCluster")

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
	svcInformer := kubeInformerFactory.Core().V1().Services()
	pvcInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	hcInformer := informerFactory.Storage().V1alpha1().HdfsClusters()
	setInformer := kubeInformerFactory.Apps().V1().StatefulSets()
	deployInformer := kubeInformerFactory.Apps().V1().Deployments()

	setControl := controller.NewRealStatefulSetControl(kubeCli, setInformer.Lister())
	svcControl := controller.NewRealServiceControl(kubeCli, svcInformer.Lister())
	pvcControl := controller.NewRealPVCControl(kubeCli, pvcInformer.Lister())
	deployControl := controller.NewRealDeploymentControl(kubeCli, deployInformer.Lister())
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
	setInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: control.addStatefulSet,
		UpdateFunc: func(old, cur interface{}) {
			control.updateStatefuSet(old, cur)
		},
		DeleteFunc: control.deleteStatefulSet,
	})
	control.hcLister = hcInformer.Lister()
	control.hcListerSynced = hcInformer.Informer().HasSynced

	control.setLister = setInformer.Lister()
	control.setListerSynced = setInformer.Informer().HasSynced
	return control
}

func (c *Controller) addStatefulSet(obj interface{}) {
	set := obj.(*apps.StatefulSet)
	ns := set.GetNamespace()
	setName := set.GetName()

	if set.DeletionTimestamp != nil {
		// on a restart of the controller manager, it's possible a new statefulset shows up in a state that
		// is already pending deletion. Prevent the statefulset from being a creation observation.
		c.deleteStatefulSet(set)
		return
	}

	// If it has a ControllerRef, that's all that matters.
	tc := c.resolveHdfsClusterFromSet(ns, set)
	if tc == nil {
		return
	}
	glog.V(4).Infof("StatefuSet %s/%s created, HdfsCluster: %s/%s", ns, setName, ns, tc.Name)
	c.enqueueHdfsCluster(tc)
}
func (c *Controller) updateStatefuSet(old, cur interface{}) {
	curSet := cur.(*apps.StatefulSet)
	oldSet := old.(*apps.StatefulSet)
	ns := curSet.GetNamespace()
	setName := curSet.GetName()
	if curSet.ResourceVersion == oldSet.ResourceVersion {
		// Periodic resync will send update events for all known statefulsets.
		// Two different versions of the same statefulset will always have different RVs.
		return
	}

	// If it has a ControllerRef, that's all that matters.
	tc := c.resolveHdfsClusterFromSet(ns, curSet)
	if tc == nil {
		return
	}
	glog.V(4).Infof("StatefulSet %s/%s updated, %+v -> %+v.", ns, setName, oldSet.Spec, curSet.Spec)
	c.enqueueHdfsCluster(tc)
}

func (c *Controller) deleteStatefulSet(obj interface{}) {
	set, ok := obj.(*apps.StatefulSet)
	ns := set.GetNamespace()
	setName := set.GetName()

	// When a delete is dropped, the relist will notice a statefuset in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		set, ok = tombstone.Obj.(*apps.StatefulSet)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a statefuset %+v", obj))
			return
		}
	}

	// If it has a TidbCluster, that's all that matters.
	tc := c.resolveHdfsClusterFromSet(ns, set)
	if tc == nil {
		return
	}
	glog.V(4).Infof("StatefulSet %s/%s deleted through %v.", ns, setName, utilruntime.GetCaller())
	c.enqueueHdfsCluster(tc)
}

func (c *Controller) enqueueHdfsCluster(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Cound't get key for object %+v: %v", obj, err))
		return
	}
	c.queue.Add(key)
}

func (c *Controller) resolveHdfsClusterFromSet(namespace string, set *apps.StatefulSet) *v1alpha1.HdfsCluster {
	controllerRef := metav1.GetControllerOf(set)
	if controllerRef == nil {
		return nil
	}

	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	tc, err := c.hcLister.HdfsClusters(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if tc.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return tc
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	glog.Info("Starting hdfscluster controller")
	defer glog.Info("Shutting down hdfscluster controller")

	if !cache.WaitForCacheSync(stopCh, c.hcListerSynced, c.setListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *Controller) worker() {
	for c.processNextWorkItem() {
		// revive:disable:empty-block
	}
}

func (c *Controller) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	if err := c.sync(key.(string)); err != nil {
		c.queue.AddRateLimited(key)
	} else {
		c.queue.Forget(key)
	}
	return true
}

func (tcc *Controller) sync(key string) error {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	tc, err := tcc.hcLister.HdfsClusters(ns).Get(name)
	if errors.IsNotFound(err) {
		glog.Infof("HdfsCluster has been deleted %v", key)
		return nil
	}
	if err != nil {
		return err
	}

	return tcc.syncHdfsCluster(tc.DeepCopy())
}

func (tcc *Controller) syncHdfsCluster(tc *v1alpha1.HdfsCluster) error {
	return tcc.control.UpdateHdfsCluster(tc)
}
