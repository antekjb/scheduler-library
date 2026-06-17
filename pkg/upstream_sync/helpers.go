package upstreamsync

import (
	"fmt"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CreatePodFromTemplate creates a new v1.Pod based on the provided pod template spec and index.
// The pod's name is constructed as "<template-name>-<index>", and it is assigned a random UUID.
func CreatePodFromTemplate(template *v1.PodTemplateSpec, index int) *v1.Pod {
	podNamePrefix := template.Name
	if podNamePrefix == "" {
		podNamePrefix = "templated-pod"
	}
	ns := template.Namespace
	if ns == "" {
		ns = "default"
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", podNamePrefix, index),
			Namespace: ns,
			UID:       types.UID(uuid.New().String()),
			Labels:    template.Labels,
		},
		Spec: template.Spec,
	}
}
