package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type HdfsCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	// Spec defines the behavior of a tidb cluster
	Spec HdfsClusterSpec `json:"spec"`
}

type HdfsClusterSpec struct {
	NameNode NameNodeSpec `json:"name_node"`
	DataNode DataNodeSpec `json:"data_node"`
}

type NameNodeSpec struct {
	Storage      string `json:"storage"`
	StorageClass string `json:"storage_class"`
}

type DataNodeSpec struct {
	Storage      string `json:"storage"`
	StorageClass string `json:"storage_class"`
	Replicas     int32  `json:"replicas"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type HdfsClusterlList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []HdfsCluster `json:"items"`
}
