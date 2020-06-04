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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	stsNamespace, stsName, configNameToUpdate     string
	stsNsIsSet, stsNameIsSet, configToUpdateIsSet bool
	clientset                                     *kubernetes.Clientset
	prometheusContainerPort                       int
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
		log.Fatalf("Error reading %s: %v\n", kubeconfig, err)
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

	stsInformerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	stsInformer := stsInformerFactory.Apps().V1().StatefulSets().Informer()
	// create list options based on name of the statefulset
	listOptions := metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", stsName),
	}

	// start watching specific statefulset events
	log.Printf("Starting watch in an infinite loop on %s/%s\n", stsNamespace, stsName)
	watchStatefulSetEvents(&listOptions)
}

func watchStatefulSetEvents(lsOpts *metav1.ListOptions) {
	namespace := stsNamespace
	for {
		// error out if runLoop() fails to create a watch client
		if err := runLoop(namespace, lsOpts); err != nil {
			log.Fatalf("Error in runLoop: %v\n", err)
		}
		// retrigger runLoop() to recretate the watch after 5 seconds
		log.Println("runLoop() broke the loop to return. Most probably the watch client's channel closed witha timeout")
		time.Sleep(5 * time.Second)
	}
}

// runLoop() creates a watch client based on the namespace and listOptions provided. Incase the watch channel is closed or the watch returns an error, the control is sent back to watchStatefulSetEvents(), which re-initiates the watch after 5 seconds delay.
func runLoop(namespace string, lsOpts *metav1.ListOptions) error {
	watcher, err := clientset.AppsV1().StatefulSets(stsNamespace).Watch(*lsOpts)
	if err != nil {
		log.Printf("Error creating a watch: %v\n", err)
		return err
	}
	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				// the watch channel has closed(probably due to timeout)
				break
			}
			if event.Type == watch.Error {
				// if the event type is error, return the error from the watch event object
				return apierrors.FromObject(event.Object)
			}
			// call event handler function
			if err := allEventHandler(&event); err != nil {
				log.Printf("Error: runLoop(): %v\n", err)
			}
		}
	}
}

func allEventHandler(event *watch.Event) error {
	// get the statefulset object from watch event
	sts, ok := event.Object.(*v1.StatefulSet)
	if !ok {
		log.Println("Error: allEventHandler: Invalid object encountered")
	}
	// print  sample output: the replica details for that statefulset
	log.Printf("Event type: %v name: %v, Ready Replicas: %v Current Replicas: %d\n", event.Type, sts.Name, sts.Status.ReadyReplicas, sts.Status.CurrentReplicas)
	// Begin configmap processing:
	log.Println("Starting configmap processing...")

	// 1. access the configmap CONFIG_TO_UPDATE data, in the same namespace
	namespace := stsNamespace
	configmapName := configNameToUpdate
	configmapKey := fmt.Sprintf("%s/%s", namespace, configmapName)
	configmap, err := clientset.CoreV1().ConfigMaps(namespace).Get(configmapName, metav1.GetOptions{})
	if err != nil {
		log.Printf("Error: allEventHandler: Error getting the configmap, %s: %v\n", configmapKey, err)
		return err
	}
	log.Printf("Successfully fetched the configmap: %s\n", configmapKey)
	// 2. get the prometheus.yml from configmap data
	promtheusConfigData, ok := configmap.Data["prometheus.yml"]
	if !ok {
		log.Printf("Error: allEventHandler: Promtheus.yml not found in configmap: %s", configmapKey)
		return fmt.Errorf("Promtheus.yml not found in configmap: %s", configmapKey)
	}
	// log.Printf("prometheus.yml:\n%s\n", promtheusConfigData)
	// 3.a. unmarshal the string prometheus.yml data to prometheus config struct objects
	var prometheusConfig *promconfig.Config
	if prometheusConfig, err = promconfig.Load(promtheusConfigData); err != nil {
		log.Printf("Error: allEventHandler: unable to unmarshal the yaml to prometheus config\n")
		return fmt.Errorf("unable to unmarshal the yaml to prometheus config")
	}
	// 3.b. reset and update the .remote_read list depending on the ReadyReplicas
	headlessServiceName := sts.Spec.ServiceName
	var count int32
	prometheusConfig.RemoteReadConfigs = nil
	for ; count < sts.Status.CurrentReplicas; count++ {
		urlStr := fmt.Sprintf("http://%s-%d.%s:%d/read", stsName, count, headlessServiceName, prometheusContainerPort)
		remoteReadURL, err := url.Parse(urlStr)
		if err != nil {
			log.Printf("Error: allEventHandler: Cannot parse remoteReadUrl: %v\n", err)
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
		log.Printf("Error: allEventHandler: Error marshalling the modified prometheusConfig\n")
		return fmt.Errorf("Error marshalling the modified prometheusConfig")
	}
	// 4. change the prometheus.yml in configmap data
	configmap.Data["prometheus.yml"] = string(newPromtheusConfigData)

	// 5. update the configmap
	configmapModified, err := clientset.CoreV1().ConfigMaps(namespace).Update(configmap)
	if err != nil {
		log.Printf("Error: allEventHandler: Error returned by api server while updating the configmap: %v\n", err)
	}
	log.Printf("Configmap data updated. Following is the configmap data: %s\n", configmapModified.Data["prometheus.yml"])
	return nil
}
