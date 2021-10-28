# Kubernetes

Kubernetes, also known as k8s, is an open-source container orchestration software. It is an application used to automate container applications deployment, scaling, network and management.

Containers are a form of packaged software that consist of the application, related library and binaries, as well as other dependencies needed for the application to run. Containers provide a means of ‘virtualisation’ but unlike typical virtualization technology, containers do not need their own operating system to run and thus, it is very lightweight and convenient.

Containers alone in production are very limited in their capability. They are static in nature and they can be difficult to manage when deployed in numbers. That is where Kubernetes comes in by playing the role in controlling and managing containers. Kubernetes provides features such as automated development of containers and self-healing to ensure that there is no downtime in production.

Other features that Kubernetes provides include:

- Service discovery and load balancing

- Storage orchestration

- Automated rollouts and rollbacks

- Resource sharing

- Self-healing

- Secret and Configuration management


# Kubernetes Cluster 

It is apparent that Kubernetes provides a framework to run distributed systems resiliently, reliably and intelligently. When Kubernetes is deployed, a cluster is formed and when a container is orchestrated by Kubernetes, it is then called a pod. A Kubernetes cluster consists of 3 major components which are Control Plane, Node and Addons.

![image21](https://user-images.githubusercontent.com/68586800/133755426-1be31687-7fdb-4fe8-aa21-e26dae69186f.png)

##### Spark on Kubernetes

The control plane is essentially the master controller of the whole Kubernetes cluster. It makes global decisions about the cluster such as pod deployment scheduling and event detections such as pod failures. Control plane has 3 subcomponents which are:
Kube-apiserver: 
Etcd
Kube-scheduler
Kube-controller-manager
Cloud-controller-manager

The node component resides on every node of the cluster and it is responsible for directly starting and running pods through Kubelet, and providing kubernetes with a container runtime environment. The subcomponents include:

- Kubelet

- Kube-proxy

- Container-runtime

Addons provide additional features such as Daemonset, Deployment set, etc, to complement Kubernetes pre-existing features. Not only that, It provides Web UI to manage and troubleshoot clusters conveniently and it also implements container resource monitoring.

# Spark on Kubernetes

Spark can be run on Kubernetes cluster by making use of Spark's native Kubernetes scheduler. Spark-submit can be used to submit a spark application to a kubernetes cluster. The process flow is as following:

1. Spark driver is created by spark and it runs within a kubernetes pod
2. Spark driver creates executors that also  run within a kubernetes pod and connects to them. It then executes the code
3. The executor pods terminate and are cleaned up as soon as the application completes. The driver pod however retains the logs and remains in ‘completed’ state in the Kubernetes API until it is manually cleared up.

Spark can run on not only Kubernetes, it can also run using its standalone cluster mode, on Hadoop YARN, Apache Mesos etc. However, the choice of using Kubernetes as the cluster manager has its competitive advantages:


## 1. Native Containerization and Isolation 


As with traditional software applications, containerization technology benefits Spark applications as well. Utilizing containers makes any applications more portable, it provides the tool for repeatable and reliable build workflows. 

Kubernetes requires users to provide images that will be built into containers within pods. It provides a container runtime environment for container support and Docker is widely used along with Kubernetes.  Starting from Spark 2.3, Spark provides a Dockerfile in the kubernetes/dockerfiles/ directory that can be used for containerization. 

Example usage is:

![image10](https://user-images.githubusercontent.com/68586800/133756226-2b8c3be4-58e5-41db-aa2c-51aa30ac7a89.png)


Spark leverages against all the benefits that containerization introduces and It can also be customised to match any specific individual application’s needs. Amongst many benefits, It enables effective dependency management and separation. Packaging all dependencies along with Spark applications in containers avoids dependency issues that are common with Spark.

Spark application dependencies can be pre-mounted into custom-built Docker images. Those dependencies can be added to the classpath by referencing them with local:// URIs and/or setting the SPARK_EXTRA_CLASSPATH environment variable in your Dockerfiles. The local:// scheme is also required when referring to dependencies in custom-built Docker images in spark-submit. Spark supports dependencies from the submission client’s local file system using the file:// scheme or without a scheme by using a full path instead, where the destination should be a Hadoop compatible filesystem. A typical example of this using S3 is via passing the following options:

![image13](https://user-images.githubusercontent.com/68586800/133756320-0c034f90-68cc-4c1a-8405-9f9362e8a7f6.png)


The app jar file will be uploaded to the S3 and then when the driver is launched it will be downloaded to the driver pod and will be added to its classpath. Spark then generates a subdir under the upload path with a random name to avoid conflicts with spark apps running at the same time. Users could manage the subdirs created according to their needs. The client scheme is supported for the application jar, and dependencies specified by properties spark.jars, spark.files and spark.archives.

Important: all client-side dependencies will be uploaded to the given path with a flat directory structure therefore file names must be unique otherwise files will be overwritten. Also make sure in the derived k8s image default ivy dir has the required access rights or modify the settings as above. The latter is also important if the users use  --packages in cluster mode.

## 2. Optimized Resource Management

Kubernetes leverage on the resource advertising feature of kubernetes. The user is responsible for configuring the Kubernetes cluster to advertise the resources available and to isolate each resource per container to avoid sharing resources between multiple containers.

Spark automatically handles Spark configs spark.{.driver/executor}.resource.
{resourceType} into the kubernetes configs. It does so as long as the Kubernetes resource type follows the Kubernetes device plugin format of vendor-domain/resourcetype. Spark users can specify the vendor using the config spark.{driver/executor}.resource.
{resourceType}. Using Kubernetes provides simplicity for resource allocation as the user does not need to configure anything else if you are using the Kubernetes Pod templates. You can schedule for example a GPU configuring the resources property in the Pod YAML file.

The availability of the device plugin framework that Kubernetes provides allows for advertising system hardwares to the application running on containers. It lets Pod to access and consume specialised hardware such as GPU or VE. The plugin is used by the respective hardware vendor to install and expose the hardware device. The user can consume these devices from the container by specifying the vendor domain and the resource type in the form of vendor_domain/device_name. This is done by specifying the limits of the resources. The example is as follows:

![image29](https://user-images.githubusercontent.com/68586800/133756503-96fa067c-42b9-4f18-9e35-dc07093effd8.png)

Kubernetes also has the concept of namespaces. Separation of cluster resources between multiple users (via resource quota) can be done by using namespaces. Spark on Kubernetes can use namespaces to launch Spark applications and this can be set by configuring the spark.kubernetes.namespace configuration. Kubernetes ResourceQuota allows the user to set limits on resources, number of objects, etc on individual namespaces. The combination of Namespaces and ResourceQuota Namespaces along with ResourceQuota can be used by the administrator to control sharing and resource allocation for spark applications running in a Kubernetes cluster.

## 3. Rich Ecosystem

Kubernetes clusters can be used for multi-tenancy and sharing by Spark application through Namespaces and Quotas. Administrative features such as Pluggable Authorization and Logging are also provided. These are all readily available without requiring any installations to the existing Kubernetes cluster. It can be done as simply as creating a container image, setting the appropriate RBAC roles for the Spark application and the  

The Kubernetes ecosystem as a whole is also enriched with powerful open source add-ons for management & monitoring. Prometheus for time-series data, Fluentd for log aggregation, and Grafana for data visualization are a few of the notable examples of how capable and promising the Kubernetes ecosystem is.



## 4. Spark Application Management

Kubernetes provides a simple application management system through the spark-submit CLI tool when it runs in cluster mode. Any jobs can be killed by the user by providing the submission ID provided when the jobs were submitted. The submission ID follows the format of namespace:driver-pod-name. If the namespace is not provided by the user, then the default namespace is set to the current k8s context being used. If the user has set a specific namespace for example: kubectl config set-context minikube --namespace=spark therefore this namespace will be used as the default namespace. All namespaces will be set by default if the user does not provide any namespaces and any operations will affect all Spark applications that match the submission ID regardless of the namespace. Not just that, as the backend for spark-submit for application management and the backend for driver submission are the same, the same properties like spark.kubernetes.context etc. can be reused.

Example:

![image20](https://user-images.githubusercontent.com/68586800/133756761-22400e4a-d694-476b-b2d2-fa0fe6beee3c.png)


Users can list the application status by using the --status flag:

![image2](https://user-images.githubusercontent.com/68586800/133757178-c887829c-2602-4081-913f-938a6c73b207.png)



Glob patterns are supported in both operations. For example, user can run:

![image18](https://user-images.githubusercontent.com/68586800/133756827-1d6b595d-bb82-48e1-af74-ff85adcc5ea3.png)

The above will kill all applications with the specific prefix.

Users can specify the grace period for pod termination via the spark.kubernetes. appKillPodDeletionGracePeriod property, using --conf as means to provide it (default value for all K8s pods is 30 secs).

# K8s Device Plugin Architecture

Kubernetes currently does not support the discovery of hardware vendor devices such as GPU TPU etc. These devices have to be advertised by the vendors themselves for Kubernetes to consume them. The reason for such design is because each specific vendor has their own architecture and design for their device, hence it is not possible for Kubernetes to consider them all. However, Kubernetes has provided vendor independent device plugin that is able to provide solutions such as:

- Discovering external devices

- Advertising the devices to the containers/pod.

- Making use of these devices and securely sharing them among the pods.

- Health Check of these devices

With this device plugin, Kubernetes is able to acknowledge vendor specific devices and utilise them for any jobs.

## Device Plugin

![image5](https://user-images.githubusercontent.com/68586800/133758351-4ae030a4-ecc1-4a1f-8d4f-726c2a04c36a.png)


The device plugin comprises of 3 main parts:

1. Registration: This advertises the device plugin to the Kubelet 

2. ListAndWatch: The device plugin advertises the list of Devices to the Kubelet. It also monitors the Health of the device and update to the Kubelet accordingly

3. Allocate: Allocate function is called when the Kubelet starts a pod and creates a container. It gives instructions to Kubelet on how to give container access to the devices

The device plugin and Kubelet communicate with each other using gRPC through Unix socket. As the device plugin starts, the gRPC server starts and creates a unix socket at the host path: /var/lib/kubelet/device-plugins/ . The device plugin is then able to find the socket for registration on the following host path: /var/lib/kubelet/device-plugins/kubelet.Sock.  During registration, the device plugin sends the following information to the Kubelet:

- The name of its Unix socket.

- The Device Plugin API version against which it was built.

- The ResourceName it wants to advertise. Here ResourceName needs to follow the extended resource naming scheme as vendor-domain/resourcetype. (For example, the NEC Vector Engine is advertised as nec.com/ve.)

Once Registered, the gRPC service implements the following interfaces:

1) `rpc GetDevicePluginOptions(Empty) returns (DevicesPluginOptions){}`
	
GetDevicePluginOptions returns the options to be communicated with Kubelet Device Manager

2) `rpc ListAndWatch(Empty) returns (stream ListAndWatchResponse){}`

ListAndWatch returns a streams of List of Devices that to be advertised to the Device Manager

3) `rpc Allocate(AllocateRequest) returns (AllocateResponse) {}`
	
Allocate is called during container creation so that the Device Plugin can run device specific operations and instruct Kubelet of the steps to make the Device available in the container

4) `rpc GetPreferredAllocation(PreferredAllocationRequest) returns (PreferredAllocationResponse) {}`

GetPreferredAllocation returns a preferred set of devices to allocate
from a list of available ones. The resulting preferred allocation is not guaranteed to be the allocation ultimately performed by the device manager. It is only designed to help the device manager make a more informed allocation decision when possible.

5) `rpc PreStartContainer(PreStartContainerRequest) returns (PreStartContainerResponse) {}`

PreStartContainer is an optional feature that is called just before each container starts by Device Plugin during registration phase. Device plugin can run device specific operations such as resetting the device before making devices available to the container.

Upon registration, the device plugin then runs in serving mode. During this mode, it monitors the health of devices and updates the health status back to kubelet. Allocate gRPC requests are also served during this mode in which the device plugin returns AllocateResponse that contains the necessary container runtime configurations to access and consume the allocated devices. During the Allocate process, the device plugin also does device-specific preparation such as GPU cleanup or as specified in the device plugin. 

## Resource Request Sequence Diagram

The device plugin now runs in serving mode as it waits for the resource request from Kube API Server. The Allocate service is served when a device request has been made. It then returns AllocateResponse that allows the pod to access and consume the requested devices. Finally the container is created with the pod and the pod can run jobs and utilize the device. The resource request sequence diagram is as follows:

![image25](https://user-images.githubusercontent.com/68586800/133759310-bc425c97-7c59-46f0-abbd-8bcb027d07d2.png)


1. The Yaml file specifies the pod configuration and the device request to the Kube API Server
2. Kube API Server exposes the Kubernetes API and the Scheduler binds the Pod to the correct node as configured in the YAML file.
3. The device plugin manager receives the device request and serve the AllocateRequest to the device plugin
4. The device plugin responses with AllocateResponce and send necessary container runtime configuration and device information
5. The Kubelet receives the information and creates the container with the pod. The pod can now utilize the device as requested
