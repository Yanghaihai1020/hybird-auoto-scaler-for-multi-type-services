package autoscaler

import (
	"sync"

	clientset "k8s.io/client-go/kubernetes"
)

func (ac *AutoscalerController) addDeployment(obj interface{}) {
	ac.HandlerAddEvents(obj)
}

func (ac *AutoscalerController) updateDeployment(old, cur interface{}) {
	ac.HandlerUpdateEvents(old, cur)
}

func (ac *AutoscalerController) deleteDeployment(obj interface{}) {
	ac.HandlerDeleteEvents(obj)
}

func (ac *AutoscalerController) addStatefulset(obj interface{}) {
	ac.HandlerAddEvents(obj)
}

func (ac *AutoscalerController) updateStatefulset(old, cur interface{}) {
	ac.HandlerUpdateEvents(old, cur)
}

func (ac *AutoscalerController) deleteStatefulset(obj interface{}) {
	ac.HandlerDeleteEvents(obj)
}

func PreAndExtractAnnotations(annotations map[string]string) (map[string]int32, error) {
	scalerAnnotations := make(map[string]int32)

	//# obtain scale policy from policy server

	return scalerAnnotations, nil
}

type SafeStoreInterface interface {
	Add(key string, obj *PodAutoscaler)
	Update(key string, obj *PodAutoscaler)
	Delete(key string)
	Get(key string) (*PodAutoscaler, bool)
}

type SafeStore struct {
	lock  sync.RWMutex
	items map[string]*PodAutoscaler
}

func (s *SafeStore) Get(key string) (*PodAutoscaler, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	item, exists := s.items[key]
	return item, exists
}

func (s *SafeStore) Add(key string, obj *PodAutoscaler) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items[key] = obj
}

func (s *SafeStore) Update(key string, obj *PodAutoscaler) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items[key] = obj
}

func (s *SafeStore) Delete(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.items[key]; ok {
		delete(s.items, key)
	}
}

func NewSafeStore() SafeStoreInterface {
	return &SafeStore{
		items: map[string]*PodAutoscaler{},
	}
}

const (
	PodAutoscaler string = "PodAutoscaler"
)

type ControllerClientBuilder interface {
	Config(name string) (*restclient.Config, error)
	ConfigOrDie(name string) *restclient.Config
	Client(name string) (clientset.Interface, error)
	ClientOrDie(name string) clientset.Interface
}

type SimpleControllerClientBuilder struct {
	ClientConfig *restclient.Config
}

type AutoscalerContext struct {
	Name        string `json:"name"`
	HpaConfig   *hpaConfig
	VpaConfig   *vpaConfig
	Annotations map[string]string `json:"annotations"`
}

func NewAutoscalerContext(obj interface{}) *AutoscalerContext {
	switch o := obj.(type) {
	case *apps.Deployment:
		return &AutoscalerContext{
			Name:        o.Name,
			HpaConfig:   o.hpaConfig,
			VpaConfig:   o.vpaConfig,
			Annotations: o.Annotations,
		}
	case *apps.StatefulSet:
		return &AutoscalerContext{
			Name:        o.Name,
			HpaConfig:   o.hpaConfig,
			VpaConfig:   o.vpaConfig,
			Annotations: o.Annotations,
		}
	default:
		return nil
	}
}

func CreatePodAutoscaler(name string, annotations map[string]int32) *PodAutoscaler {
	scaler := &PodAutoscaler{
		PodAutoscalerSpec{
			ServiceName: name,
			Replicas:    annotations[Replicas],
			CpuCapacity: annotations[CpuCapacity],
			MemCapacity: annotations[MemCapacity],
			Metrics:     parseMetrics(annotations),
		},
	}

	return scaler
}

func IsNeedForScalers(annotations map[string]string) bool {
	if annotations == nil || len(annotations) == 0 {
		return false
	}

	for aKey := range annotations {
		if aKey != "" {
			return true
		}
	}

	return false
}
