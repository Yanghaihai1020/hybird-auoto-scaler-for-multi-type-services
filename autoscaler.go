package autoscaler

import (
	"context"
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

var predictResults []*predictResult

type predictResult struct {
	time             *time.Time
	cpuTotalUsage    int
	memoryTotalUsage int
}

type metricsMonitor struct {
	time           time.Time
	cpuUsage       int
	cpuCapacity    int
	memoryUsage    int
	memoryCapacity int
}

type scalePolicy struct {
	hConfig *hpaConfig
	vConfig *vpaConfig
}

type hpaConfig struct {
	replicaNum int
}

type vpaConfig struct {
	cpuCapacity    int
	memoryCapacity int
}

const (
	maxRetries = 15

	AddEvent           string = "Add"
	UpdateEvent        string = "Update"
	DeleteEvent        string = "Delete"
	RecoverDeleteEvent string = "RecoverDelete"
	RecoverUpdateEvent string = "RecoverUpdate"

	ScalerEvent string = "ScalerEvent"
)

const (
	appsAPIVersion string = "apps/v1"

	Deployment              string = "Deployment"
	StatefulSet             string = "StatefulSet"
	HorizontalPodAutoscaler string = "HorizontalPodAutoscaler"
	VerticalPodAutoscaler   string = "VerticalPodAutoscaler"
)

type AutoscalerController struct {
	client        clientset.Interface
	eventRecorder record.EventRecorder

	syncHandler       func(scalerKey string) error
	enqueueAutoscaler func(scaler *PodAutoScaler)

	dLister appslisters.DeploymentLister
	sLister appslisters.StatefulSetLister

	dListerSynced cache.InformerSynced
	sListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
	store Scalerstore.SafeStoreInterface
}

func getMonitorMetrics() []*metricsMonitor {
	re := make([]*metricsMonitor, 0)
	//get monioting metrics from metrics server
	return re
}

func NewAutoscalerController(
	dInformer appsinformers.DeploymentInformer,
	sInformer appsinformers.StatefulSetInformer,
	client clientset.Interface) (*AutoscalerController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: client.CoreV1().Events("")})

	ac := &AutoscalerController{
		client:        client,
		eventRecorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "autoscaler-controller"}),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "autoscaler"),
		store:         Scalerstore.NewSafeStore(),
	}

	// Stateless service
	dInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ac.addDeployment,
		UpdateFunc: ac.updateDeployment,
		DeleteFunc: ac.deleteDeployment,
	})

	// Stateful service
	sInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ac.addStatefulset,
		UpdateFunc: ac.updateStatefulset,
		DeleteFunc: ac.deleteStatefulset,
	})

	ac.dLister = dInformer.Lister()
	ac.sLister = sInformer.Lister()

	ac.syncHandler = ac.syncAutoscalers
	ac.enqueueAutoscaler = ac.enqueue

	ac.dListerSynced = dInformer.Informer().HasSynced
	ac.sListerSynced = sInformer.Informer().HasSynced

	return ac, nil
}

func (ac *AutoscalerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ac.queue.ShutDown()

	if !cache.WaitForNamedCacheSync("Scaler-autoscaler-manager", stopCh, ac.dListerSynced, ac.sListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(ac.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (ac *AutoscalerController) addScaler(obj interface{}) {}

func (ac *AutoscalerController) updateScaler(old, cur interface{}) {
	oldH := old.(*PodAutoscaler)
	curH := cur.(*PodAutoscaler)
	if oldH.ResourceVersion == curH.ResourceVersion {
		return
	}

	key, err := controller.KeyFunc(curH)
	if err != nil {
		return
	}
	ac.InsertScalerAnnotation(curH, RecoverUpdateEvent)
	ac.store.Update(key, curH)

	ac.enqueueAutoscaler(curH)
}

func (ac *AutoscalerController) deleteScaler(obj interface{}) {
	h := obj.(*aPodAutoscaler)

	key, err := controller.KeyFunc(h)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", h, err))
		return
	}
	ac.InsertScalerAnnotation(h, RecoverDeleteEvent)
	ac.store.Update(key, h)

	ac.enqueueAutoscaler(h)
}

func (ac *AutoscalerController) enqueue(scaler *PodAutoscaler) {
	key, err := controller.KeyFunc(scaler)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", scaler, err))
		return
	}

	ac.queue.Add(key)
}

func (ac *AutoscalerController) worker() {
	for ac.processNextWorkItem() {
	}
}

func (ac *AutoscalerController) processNextWorkItem() bool {
	key, quit := ac.queue.Get()
	if quit {
		return false
	}
	defer ac.queue.Done(key)

	err := ac.syncHandler(key.(string))
	ac.handleErr(err, key)
	return true
}

func (ac *AutoscalerController) syncAutoscalers(key string) error {
	defer ac.store.Delete(key)

	scaler, exists := ac.store.Get(key)
	if !exists {
		return nil
	}

	var err error
	event := ac.PopScalerAnnotation(scaler)

	switch event {
	case AddEvent:
		_, err = CreateScaler(context.TODO(), scaler)
		if errors.IsAlreadyExists(err) {
			return nil
		}
	case UpdateEvent:
		_, err = UpdateScaler(context.TODO())
		if errors.IsNotFound(err) {
			return nil
		}
	case DeleteEvent:
		err = DeleteScaler(context.TODO(), scaler.Name)
		if errors.IsNotFound(err) {
			return nil
		}
	case RecoverUpdateEvent:
		newScaler, err := GetNewestScalerFromResource(scaler)
		if err != nil {
			return err
		}
		if newScaler == nil {
			return nil
		}
		if reflect.DeepEqual(scaler.Spec, newScaler.Spec) {
			return nil
		}

		_, err = ac.client.AutoscalingV2beta2().HorizontalPodAutoscalers(newScaler.Namespace).Update(context.TODO(), newScaler, metav1.UpdateOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
	case RecoverDeleteEvent:
		newScaler, err := GetNewestScalerFromResource(scaler)
		if err != nil {
			return err
		}
		if newScaler == nil {
			return nil
		}

		_, err = CreateScaler(context.TODO(), newScaler, metav1.CreateOptions{})
		if err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return err
		}
	default:
	}

	return err
}

func (ac *AutoscalerController) GetNewestScalerFromResource(scaler *PodAutoScaler) (*PodAutoScaler, error) {
	var annotations map[string]string
	var uid types.UID
	kind := scaler.Spec.ScaleTargetRef.Kind
	switch kind {
	case Deployment:
		d, err := GetScaler(scaler.Name)
		if err != nil {
			return nil, err
		}
		uid = d.UID
		annotations = d.Annotations
	case StatefulSet:
		s, err := GetScaler(scaler.Name)
		if err != nil {
			return nil, err
		}
		uid = s.UID
		annotations = s.Annotations
	}
	if !controller.IsNeedForScalers(annotations) {
		return nil, nil
	}

	scalerAnnotations, err := controller.PreAndExtractAnnotations(annotations)
	if err != nil {
		return nil, err
	}

	return CreatePodAutoscaler(scaler.Name, scalerAnnotations), nil
}

func (ac *AutoscalerController) InsertScalerAnnotation(scaler *PodAutoScaler, event string) {
	if scaler.Annotations == nil {
		scaler.Annotations = map[string]string{
			ScalerEvent: event,
		}
		return
	}
	scaler.Annotations[ScalerEvent] = event
}

func (ac *AutoscalerController) PopScalerAnnotation(scaler *PodAutoScaler) string {
	event, exists := scaler.Annotations[ScalerEvent]
	if exists {
		delete(scaler.Annotations, ScalerEvent)
	}
	return event
}

func (ac *AutoscalerController) HandlerAddEvents(obj interface{}) {
	ascCtx := controller.NewAutoscalerContext(obj)
	if !controller.IsNeedForScalers(ascCtx.Annotations) {
		return
	}

	scalerAnnotations, err := controller.PreAndExtractAnnotations(ascCtx.Annotations)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	scaler := controller.CreatePodAutoscaler(ascCtx.Name, scalerAnnotations)
	if scaler == nil {
		return
	}

	key, err := controller.KeyFunc(scaler)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", scaler, err))
		return
	}
	ac.InsertScalerAnnotation(scaler, AddEvent)
	ac.store.Update(key, scaler)

	ac.enqueueAutoscaler(scaler)
}

func (ac *AutoscalerController) HandlerUpdateEvents(old, cur interface{}) {
	oldCtx := controller.NewAutoscalerContext(old)
	curCtx := controller.NewAutoscalerContext(cur)

	if reflect.DeepEqual(oldCtx.Annotations, curCtx.Annotations) {
		return
	}
	oldExists := controller.IsNeedForScalers(oldCtx.Annotations)
	curExists := controller.IsNeedForScalers(curCtx.Annotations)

	if oldExists && !curExists {
		scaler, err := GetScaler(oldCtx.Name)
		if err != nil {
			if errors.IsNotFound(err) {
				return
			}
			utilruntime.HandleError(err)
			return
		}

		key, err := controller.KeyFunc(scaler)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", scaler, err))
			return
		}
		ac.InsertScalerAnnotation(scaler, DeleteEvent)
		ac.store.Add(key, scaler)

		ac.enqueueAutoscaler(scaler)
		return
	}

	curAnnotations, err := controller.PreAndExtractAnnotations(curCtx.Annotations)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	scaler := controller.CreatePodAutoscaler(curCtx.Name, curAnnotations)
	key, err := controller.KeyFunc(scaler)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", scaler, err))
		return
	}

	if !oldExists && curExists {
		ac.InsertScalerAnnotation(scaler, AddEvent)
	} else if oldExists && curExists {
		ac.InsertScalerAnnotation(scaler, UpdateEvent)
	}
	ac.store.Add(key, scaler)

	ac.enqueueAutoscaler(scaler)
}

func (ac *AutoscalerController) HandlerDeleteEvents(obj interface{}) {
	ascCtx := controller.NewAutoscalerContext(obj)

	scaler, err := GetScaler(ascCtx.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return
		}
		utilruntime.HandleError(err)
		return
	}
	if !controller.IsOwnerReference(ascCtx.UID, scaler.OwnerReferences) {
		return
	}

	key, err := controller.KeyFunc(scaler)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", scaler, err))
		return
	}
	ac.InsertScalerAnnotation(scaler, DeleteEvent)
	ac.store.Update(key, scaler)

	ac.enqueueAutoscaler(scaler)
}

func (ac *AutoscalerController) handleErr(err error, key interface{}) {
	if err == nil {
		ac.queue.Forget(key)
		return
	}

	if ac.queue.NumRequeues(key) < maxRetries {
		ac.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	ac.queue.Forget(key)
}
