package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"
	yaml "gopkg.in/yaml.v2"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

var (
	stsNamespace, stsName, configNameToUpdate     string
	stsNsIsSet, stsNameIsSet, configToUpdateIsSet bool
	clientset                                     *kubernetes.Clientset
	prometheusContainerPort                       int
	lastSeenCurrentReplicas                       int32
)

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

	// create a rest config
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Printf("Error reading %s: %v\n", kubeconfig, err)
		log.Println("Trying InClusterConfig()")
		config, err = clientcmd.BuildConfigFromFlags("", "")
		if err != nil {
			log.Fatalf("Error getting inclusterconfig: %v\n", err)
		}
	}

	// create a clientset(serializer client)
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating clientset: %v", err)
	}

	stsInformerFactory := informers.NewFilteredSharedInformerFactory(
		clientset,
		30*time.Second,
		stsNamespace,
		func(opt *metav1.ListOptions) {
			opt.FieldSelector = fmt.Sprintf("metadata.name=%s", stsName)
		},
	)
	stsInformer := stsInformerFactory.Apps().V1().StatefulSets().Informer()

	stsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			klog.Info("AddFunc triggered")
			if err := updateAccumulatorConfig(configNameToUpdate, stsNamespace, obj.(*v1.StatefulSet).Spec.ServiceName, *obj.(*v1.StatefulSet).Spec.Replicas); err != nil {
				klog.Error(err)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			klog.Info("UpdateFunc triggered")
			if oldObj.(*v1.StatefulSet).Status.CurrentReplicas != newObj.(*v1.StatefulSet).Status.CurrentReplicas {
				if err := updateAccumulatorConfig(configNameToUpdate, stsNamespace, newObj.(*v1.StatefulSet).Spec.ServiceName, newObj.(*v1.StatefulSet).Status.CurrentReplicas); err != nil {
					klog.Error(err)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			klog.Info("DeleteFunc triggered")
			if err := updateAccumulatorConfig(configNameToUpdate, stsNamespace, obj.(*v1.StatefulSet).Spec.ServiceName, 0); err != nil {
				klog.Error(err)
			}
		},
	})
	stop := make(chan struct{})
	defer close(stop)
	stsInformerFactory.Start(stop)
	for {
		time.Sleep(time.Second)
	}
}

func updateAccumulatorConfig(configmapName, namespace, headlessServiceName string, remoteReadCount int32) error {
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
	log.Printf("Configmap data updated. Following is the configmap data:\n%s\n", configmapModified.Data["prometheus.yml"])
	return nil
}
