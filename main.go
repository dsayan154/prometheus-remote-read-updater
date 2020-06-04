package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"
	yaml "gopkg.in/yaml.v2"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

var (
	stsNamespace, stsName, configNameToUpdate     string
	stsNsIsSet, stsNameIsSet, configToUpdateIsSet bool
	clientset                                     *kubernetes.Clientset
	prometheusContainerPort                       int
)

const (
	maxRequeueCount = 5
)

//PrometheusShardInfo holds the details of the prometheus shards statefulsets, these values might change with every call to processItem()
type PrometheusShardInfo struct {
	headlessServiceName     string
	lastSeenCurrentReplicas int32
}

//AccumulatorConfig holds the details of the accumulator prometheus
type AccumulatorConfig struct {
	configFileName     string
	configmapName      string
	configmapNamespace string
	promtheusShardInfo PrometheusShardInfo
}

//Controller holds the custom controller details
type Controller struct {
	indexer           cache.Indexer
	queue             workqueue.RateLimitingInterface
	informer          cache.Controller
	accumulatorConfig *AccumulatorConfig
}

//NewController returns a Controller object based on the received parameters
func NewController(indexer cache.Indexer, queue workqueue.RateLimitingInterface, informer cache.Controller, accumulatorConfig *AccumulatorConfig) *Controller {
	return &Controller{
		indexer:           indexer,
		queue:             queue,
		informer:          informer,
		accumulatorConfig: accumulatorConfig,
	}
}

//ProcessNextItem processes next item from the controller's queue, otherwise waits till next item arrives in the queue
func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	err := c.processItem(key.(string))
	c.handlerErr(err, key)
	return true
}

func (c *Controller) processItem(key string) error {
	//Move allEventHandler() function here
	obj, exists, err := c.indexer.GetByKey(key)
	sts, ok := obj.(*v1.StatefulSet)
	if !ok {
		klog.Errorf("Invalid object encountered\n")
		return fmt.Errorf("Invalid object encountered")
	}
	if err != nil {
		klog.Errorf("Fetching statefulset object with key %s from store failed with %v\n\n", key, err)
		return err
	}
	if !exists {
		klog.Info("Statefulset object with key %v doesn't exist anymore", key)
		// Add onDelete logic for the statefulset
		if err := c.UpdateAccumulatorConfig(c.accumulatorConfig.configmapName, c.accumulatorConfig.configmapNamespace, c.accumulatorConfig.promtheusShardInfo.headlessServiceName, 0); err != nil {
			klog.Errorf("Error updating the accumulator config on statefulset delete: %v", err)
			return err
		}
		return nil
	}
	//promtheusShardInfo would be populated once a successful fetching of a queue item
	c.accumulatorConfig.promtheusShardInfo.headlessServiceName = sts.Spec.ServiceName
	c.accumulatorConfig.promtheusShardInfo.lastSeenCurrentReplicas = sts.Status.CurrentReplicas
	klog.Infof("Statefulset %v synced/added/updated\n", key)
	currentReplicas := sts.Status.CurrentReplicas
	readyReplicas := sts.Status.ReadyReplicas
	if !(currentReplicas == readyReplicas) {
		klog.Infof("Current Replicas: %v Ready Replicas: %v .. Proceeding to update accumulator config\n")
		if err := c.UpdateAccumulatorConfig(c.accumulatorConfig.configmapName, sts.GetNamespace(), c.accumulatorConfig.configmapNamespace, currentReplicas); err != nil {
			return err
		}
	} else {
		klog.Infof("Current Replicas: %v Read Replicas: %v .. Skipping\n", currentReplicas, readyReplicas)
	}
	return nil
}

//UpdateAccumulatorConfig overwrites remote_read configuration of accumlator prometheus depending on CurrentReplicas of the shard statefulset
func (c *Controller) UpdateAccumulatorConfig(configmapName, namespace, headlessServiceName string, remoteReadCount int32) error {
	// TODO:
	// 1. Updation should happen based on the headlessServiceName and remoteReadCount provided. All remote_read config should not be overwritten

	klog.Infoln("Received a config update request")
	klog.Infoln("Starting configmap processing...")

	// 1. access the configmap CONFIG_TO_UPDATE data, in the same namespace
	configmapKey := fmt.Sprintf("%s/%s", namespace, configmapName)
	configmap, err := clientset.CoreV1().ConfigMaps(namespace).Get(configmapName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Error getting the configmap, %s: %v\n", configmapKey, err)
		return err
	}
	klog.Infof("Successfully fetched the configmap: %s\n", configmapKey)
	// 2. get the prometheus.yml from configmap data
	promtheusConfigData, ok := configmap.Data["prometheus.yml"]
	if !ok {
		klog.Errorf("promtheus.yml not found in configmap: %s", configmapKey)
		return fmt.Errorf("promtheus.yml not found in configmap: %s", configmapKey)
	}
	// log.Printf("prometheus.yml:\n%s\n", promtheusConfigData)
	// 3.a. unmarshal the string prometheus.yml data to prometheus config struct objects
	var prometheusConfig *promconfig.Config
	if prometheusConfig, err = promconfig.Load(promtheusConfigData); err != nil {
		klog.Errorf("unable to unmarshal the yaml to prometheus config\n")
		return err
	}
	// 3.b. reset and update the .remote_read list depending on the ReadyReplicas
	var count int32
	prometheusConfig.RemoteReadConfigs = nil
	for ; count < remoteReadCount; count++ {
		urlStr := fmt.Sprintf("http://%s-%d.%s:%d/read", stsName, count, headlessServiceName, prometheusContainerPort)
		remoteReadURL, err := url.Parse(urlStr)
		if err != nil {
			klog.Errorf("cannot parse remoteReadUrl: %v\n", err)
			return err
		}
		remoteReadConfig := promconfig.RemoteReadConfig{
			URL:        &config.URL{URL: remoteReadURL},
			ReadRecent: true,
		}
		prometheusConfig.RemoteReadConfigs = append(prometheusConfig.RemoteReadConfigs, &remoteReadConfig)
	}
	newPromtheusConfigData, err := yaml.Marshal(prometheusConfig)
	if err != nil {
		klog.Errorf("error marshalling the modified prometheusConfig\n")
		return err
	}
	// 4. change the prometheus.yml in configmap data
	configmap.Data["prometheus.yml"] = string(newPromtheusConfigData)

	// 5. update the configmap
	configmapModified, err := clientset.CoreV1().ConfigMaps(namespace).Update(configmap)
	if err != nil {
		klog.Errorf("error returned by api server while updating the configmap: %v\n", err)
		return err
	}
	klog.Infof("Configmap data updated. Following is the configmap data: %s\n", configmapModified.Data["prometheus.yml"])
	return nil
}

func (c *Controller) handlerErr(err error, key interface{}) {
	//handle error generated from processItem()
	if err == nil {
		c.queue.Forget(key)
		return
	}
	if c.queue.NumRequeues(key) < maxRequeueCount {
		c.queue.AddRateLimited(key)
		return
	}
	c.queue.Forget(key)
	runtime.HandleError(err)
	klog.Warningf("Dropping queue item: %v after %v retries\n", key, maxRequeueCount)
}

//Run starts the controller
func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()
	klog.Info("Staring Prometheus-remote-read-updater controller...")
	go c.informer.Run(stopCh)
	klog.Info("Going to wait till cache is synced")
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}
	klog.Info("Prometheus-remote-read-updater controller synced and ready...")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	<-stopCh
	klog.Info("Stopping Prometheus-remote-read-updater controller...")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func main() {
	// prequesites
	stsNamespace, stsNsIsSet = os.LookupEnv("NAMESPACE")
	stsName, stsNameIsSet = os.LookupEnv("STS_NAME")
	configNameToUpdate, configToUpdateIsSet = os.LookupEnv("CONFIG_TO_UPDATE")
	if !stsNsIsSet || !stsNameIsSet || !configToUpdateIsSet {
		log.Fatalln("NAMESPACE/STS_NAME/CONFIG_TO_UPDATE environment variable not set(required). Exiting...")
	}
	// optional
	prometheusContainerPortStr, ok := os.LookupEnv("PROM_CONTAINER_PORT")
	if !ok {
		prometheusContainerPortStr = "9090"
	}
	var err error
	prometheusContainerPort, err = strconv.Atoi(prometheusContainerPortStr)
	if err != nil {
		log.Fatalf("Error: main: Cannot convert prometheusContainerPortStr to integer\n")
	}

	var kubeconfig string
	var master string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.Parse()
	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatal(err)
	}

	// create the pod watcher
	stsListWatcher := cache.NewListWatchFromClient(clientset.AppsV1().RESTClient(), "statefulsets", stsNamespace, fields.OneTermEqualSelector("metadata.name", stsName))

	//crate a workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	//Bind the workqueue to the cache with the help of an informer
	stsInformer := cache.NewSharedIndexInformer(stsListWatcher, &v1.StatefulSet{}, 30*time.Minute, cache.Indexers{})
	stsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if oldObj.(*v1.StatefulSet).Status.CurrentReplicas != newObj.(*v1.StatefulSet).Status.CurrentReplicas {
				key, err := cache.MetaNamespaceIndexFunc(newObj)
				if err == nil {
					queue.Add(key)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	})
	//create a controller object
	controller := NewController(stsInformer.GetIndexer(), queue, stsInformer.GetController(), &AccumulatorConfig{
		configFileName:     configNameToUpdate,
		configmapName:      configNameToUpdate,
		configmapNamespace: stsNamespace,
	})
	//Now let's start the controller
	stop := make(chan struct{})
	defer close(stop)
	go controller.Run(1, stop)

	//wait forever
	select {}
}
