package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	stsNamespace, stsName, configNameToUpdate     string
	stsNsIsSet, stsNameIsSet, configToUpdateIsSet bool
	clientset                                     *kubernetes.Clientset
)

func main() {
	// prequesites
	stsNamespace, stsNsIsSet = os.LookupEnv("NAMESPACE")
	stsName, stsNameIsSet = os.LookupEnv("STS_NAME")
	configNameToUpdate, configToUpdateIsSet = os.LookupEnv("CONFIG_TO_UPDATE")
	if !stsNsIsSet || !stsNameIsSet || !configToUpdateIsSet {
		log.Fatalln("NAMESPACE/STS_NAME/CONFIG_TO_UPDATE environment variable not set(required). Exiting...")
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
				// if the event type is error, the return the error from the watch event object
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
	// 1. access the configmap CONFIG_TO_UPDATE data, in the same namespace
	// 2. get the prometheus.yml from configmap data
	// 3. convert the string prometheus.yml to yaml. unmarshal the yaml. update the .remote_read list depending on the ReadyReplicas
	// 4. create a slice of map[string]string
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
	promtheusConfig, ok := configmap.Data["prometheus.yml"]
	if !ok {
		log.Printf("Error: allEventHandler: Promtheus.yml not found in configmap: %s, %s", configmapKey, promtheusConfig)
		return fmt.Errorf("Promtheus.yml not found in configmap: %s, %s", configmapKey, promtheusConfig)
	}
	log.Printf("prometheus.yml:\n%s\n", promtheusConfig)
	// 3. convert the string prometheus.yml to yaml. unmarshal the yaml. update the .remote_read list depending on the ReadyReplicas

	return nil
}
