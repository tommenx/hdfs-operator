package manager

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type dataNodeManager struct {
	setControl     controller.StatefulSetControlInterface
	svcControl     controller.ServiceControlInterface
	namenodeScaler Scaler
}

func NewDataNodeManager(
	setControl controller.StatefulSetControlInterface,
	svcControl controller.ServiceControlInterface,
	namenodeScaler Scaler,
) Manager {
	return &dataNodeManager{
		setControl,
		svcControl,
		namenodeScaler,
	}
}

func (dnm *dataNodeManager) Sync(hc *v1alpha1.HdfsCluster) error {
	if err := dnm.SyncDatanodeHeadlessService(hc); err != nil {
		glog.Errorf("sync data node headless service error, err=%+v", err)
		return err
	}
	if err := dnm.SyncDatanodeStatefulSet(hc); err != nil {
		glog.Errorf("sync data node statefulset error, err=%+v", err)
		return err
	}
	glog.Info("sync data node success")
	return nil
}

//TODO
// 检查是否已经存在
func (dnm *dataNodeManager) SyncDatanodeHeadlessService(hc *v1alpha1.HdfsCluster) error {
	svc := dnm.getDatanodeHeadlessService(hc)
	err := dnm.svcControl.CreateService(hc, svc)
	if err != nil {
		glog.Errorf("sync data node service, err=%+v", err)
		return err
	}
	glog.Infof("sync data node  service success")
	return nil
}

//TODO
// 检查是否已经存在
func (dnm *dataNodeManager) SyncDatanodeStatefulSet(hc *v1alpha1.HdfsCluster) error {
	set := dnm.getDatanodeStatefulset(hc)
	err := dnm.setControl.CreateStatefulSet(hc, set)
	if err != nil {
		glog.Errorf("sync data node statefulset, err=%+v", err)
		return err
	}
	glog.Infof("sync data node statefulset success")
	return nil
}

func (dnm *dataNodeManager) getDatanodeHeadlessService(hc *v1alpha1.HdfsCluster) *corev1.Service {
	name := hc.Name
	ns := hc.Namespace
	svcName := controller.DataNodeServiceName(name)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          controller.DataNodeLabel(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{
				{
					Port:     80,
					Protocol: corev1.ProtocolTCP,
				},
			},
			Selector: controller.DataNodeLabel(),
		},
	}
}

func (dnm *dataNodeManager) getDatanodeStatefulset(hc *v1alpha1.HdfsCluster) *appsv1.StatefulSet {
	name := hc.Name
	ns := hc.Namespace
	setName := controller.DataNodeSetName(name)
	replicas := hc.Spec.DataNode.Replicas
	scName := hc.Spec.DataNode.StorageClass
	svcName := controller.DataNodeServiceName(name)
	namenodeSvc := controller.NameNodeServiceName(name)
	sz := hc.Spec.DataNode.Storage
	var q resource.Quantity
	q, _ = resource.ParseQuantity(sz)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            setName,
			Namespace:       ns,
			Labels:          controller.DataNodeLabel(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: svcName,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "datanode",
							Image:           "uhopper/hadoop-datanode:2.7.2",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{
									Name:  "CORE_CONF_fs_defaultFS",
									Value: fmt.Sprintf("http://%s:8020", namenodeSvc),
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "hdfs-data",
									MountPath: "/hadoop/dfs/data",
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "hdfs-data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: &scName,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: q,
							},
						},
					},
				},
			},
		},
	}
}

func (m *dataNodeManager) CheckStatus() bool {
	return false
}
