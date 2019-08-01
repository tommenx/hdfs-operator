package controller

import (
	"fmt"
	"github.com/tommenx/hdfs-operator/pkg/apis/storage.io/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	controllerKind = v1alpha1.SchemeGroupVersion.WithKind("HdfsCluster")
)

func GetOwnerRef(tc *v1alpha1.HdfsCluster) metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true
	return metav1.OwnerReference{
		APIVersion:         controllerKind.GroupVersion().String(),
		Kind:               controllerKind.Kind,
		Name:               tc.GetName(),
		UID:                tc.GetUID(),
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
}

func NameNodeServiceName(clusterName string) string {
	return fmt.Sprintf("%snn", clusterName)
}

func NameNodePVCName(clusterName string) string {
	return fmt.Sprintf("%s-namenode", clusterName)
}

func NameNodeDeployment(clusterName string) string {
	return fmt.Sprintf("%s-namenode", clusterName)
}

func DataNodeServiceName(clusterName string) string {
	return fmt.Sprintf("%sdn", clusterName)
}

func DataNodeSetName(clusterName string) string {
	return fmt.Sprintf("%s-datanode", clusterName)
}

func DataNodeLabel() map[string]string {
	labels := make(map[string]string)
	labels["app"] = "datanode"
	return labels
}

func NameNodeLabel() map[string]string {
	label := make(map[string]string)
	label["app"] = "namenode"
	return label
}
