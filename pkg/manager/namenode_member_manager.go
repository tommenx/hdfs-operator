package manager

import (
	"github.com/golang/glog"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	"github.com/tommenx/hdfs-operator/pkg/controller"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type nameNodeManager struct {
	deploymentControl controller.DeploymentControlInterface
	pvcControl        controller.PVCControlInterface
	svcControl        controller.ServiceControlInterface
	podControl        controller.PodControlInterface
}

func NewNameNodeManager(
	deployControl controller.DeploymentControlInterface,
	pvcControl controller.PVCControlInterface,
	podControl controller.PodControlInterface,
	svcControl controller.ServiceControlInterface,
) Manager {
	return &nameNodeManager{
		deploymentControl: deployControl,
		pvcControl:        pvcControl,
		podControl:        podControl,
		svcControl:        svcControl,
	}
}

func (nnm *nameNodeManager) Sync(hc *v1alpha1.HdfsCluster) error {
	if err := nnm.SyncNameNodeService(hc); err != nil {
		glog.Errorf("create name node service error, err=%+v", err)
		return err
	}
	if err := nnm.SyncNameNodePVC(hc); err != nil {
		glog.Errorf("create name node pvc error, err=%+v", err)
		return err
	}
	return nnm.SyncNameNodeDeployment(hc)
}

func (nnm *nameNodeManager) CheckStatus() bool {
	_, status, err := nnm.podControl.CheckPodsStatus(controller.NameNodeLabel())
	if err != nil {
		glog.Errorf("check pod status error, err=%+v", err)
		return false
	}
	if status != nil {
		for name, state := range status {
			glog.Errorf("%s status is %s", name, state)
			return false
		}
	}
	return true
}

//TODO
// 最好需要判断集群中是否已经存在了namenode service
func (nnm *nameNodeManager) SyncNameNodeService(hc *v1alpha1.HdfsCluster) error {
	svc := nnm.getNameNodeService(hc)
	err := nnm.svcControl.CreateService(hc, svc)
	if err != nil {
		glog.Errorf("sync name node service, err=%+v", err)
		return err
	}
	glog.Infof("sync name node service success")
	return nil
}

//TODO
// 最好判断集群中手否存在了namenode pvc
func (nnm *nameNodeManager) SyncNameNodePVC(hc *v1alpha1.HdfsCluster) error {
	pvc := nnm.getNameNodePVC(hc)
	err := nnm.pvcControl.CreatePVC(hc, pvc)
	if err != nil {
		glog.Errorf("create name node pvc error, err=%+v", err)
		return err
	}
	glog.Infof("create name node pvc success")
	return nil
}

func (nnm *nameNodeManager) SyncNameNodeDeployment(hc *v1alpha1.HdfsCluster) error {
	deployment := nnm.getNameNodeDeployment(hc)
	err := nnm.deploymentControl.CreateDeployment(hc, deployment)
	if err != nil {
		glog.Errorf("create name node deployment error, err=%+v", err)
		return err
	}
	glog.Infof("create name node deployment success")
	return nil
}

func (nnm *nameNodeManager) getNameNodeService(hc *v1alpha1.HdfsCluster) *corev1.Service {
	ns := hc.Namespace
	tcName := hc.Name
	svcName := controller.NameNodeServiceName(tcName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          controller.NameNodeLabel(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: corev1.ServiceSpec{
			Type: "NodePort",
			Ports: []corev1.ServicePort{
				{
					Name:       "nn-rpc",
					Port:       8020,
					TargetPort: intstr.FromInt(8020),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "nn-web",
					Port:       80,
					TargetPort: intstr.FromInt(50070),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: controller.NameNodeLabel(),
		},
	}
}

func (nnm *nameNodeManager) getNameNodePVC(hc *v1alpha1.HdfsCluster) *corev1.PersistentVolumeClaim {
	ns := hc.Namespace
	name := hc.Name
	pvcName := controller.NameNodePVCName(name)
	sz := hc.Spec.NameNode.Storage
	var q resource.Quantity
	q, _ = resource.ParseQuantity(sz)
	sc := hc.Spec.NameNode.StorageClass
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            pvcName,
			Namespace:       ns,
			Labels:          controller.NameNodeLabel(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: &sc,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: q,
				},
			},
		},
	}
}

func (nnm *nameNodeManager) getNameNodeDeployment(hc *v1alpha1.HdfsCluster) *apps.Deployment {
	ns := hc.Namespace
	name := hc.Name
	deploymentName := controller.NameNodeDeployment(name)
	replicas := int32(1)
	return &apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            deploymentName,
			Namespace:       ns,
			Labels:          controller.NameNodeLabel(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: apps.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: controller.NameNodeLabel(),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: controller.NameNodeLabel(),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "namenode",
							Image: "uhopper/hadoop-namenode:2.7.2",
							Env: []corev1.EnvVar{
								{Name: "CLUSTER_NAME", Value: name},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8020,
									Name:          "nn-rpc",
								},
								{
									ContainerPort: 50070,
									Name:          "nn-web",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "hdfs-name",
									MountPath: "/hadoop/dfs/name",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "hdfs-name",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: controller.NameNodePVCName(name),
								},
							},
						},
					},
				},
			},
		},
	}
}
