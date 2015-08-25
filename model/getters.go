// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"errors"
	"fmt"
	"time"

	"k8s.io/heapster/store/daystore"
	"k8s.io/heapster/store/statstore"
)

// Errors for the Getter methods
var (
	errModelEmpty      = errors.New("the model is not populated yet")
	errNoEntityMetrics = errors.New("the requested entity does not have any metrics yet")
	errInvalidNode     = errors.New("the requested node is not present in the cluster")
	errNoSuchMetric    = errors.New("the requested metric is not present in the model")
	errNoSuchNamespace = errors.New("the requested namespace is not present in the cluster")
	errNoSuchPod       = errors.New("the requested pod is not present in the specified namespace")
	errNoSuchContainer = errors.New("the requested container is not present in the model")
)

// GetClusterMetric returns a metric of the cluster entity, along with the latest timestamp.
// GetClusterMetric returns a slice of TimePoints for that metric, with times starting AFTER the starting timestamp.
func (rc *realCluster) GetClusterMetric(req ClusterMetricRequest) ([]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	if len(rc.Metrics) == 0 {
		return nil, zeroTime, errNoEntityMetrics
	}

	ts, ok := rc.Metrics[req.MetricName]
	if !ok {
		return nil, zeroTime, errNoSuchMetric
	}
	res := (*ts).Hour.Get(req.Start, req.End)
	return res, rc.timestamp, nil
}

// GetNodeMetric returns a metric of a node entity, along with the latest timestamp.
// GetNodeMetric returns a slice of TimePoints for that metric, with times starting AFTER the starting timestamp.
func (rc *realCluster) GetNodeMetric(req NodeMetricRequest) ([]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	if len(rc.Nodes) == 0 {
		return nil, zeroTime, errModelEmpty
	}
	if _, ok := rc.Nodes[req.NodeName]; !ok {
		return nil, zeroTime, errInvalidNode
	}
	if len(rc.Nodes[req.NodeName].Metrics) == 0 {
		return nil, zeroTime, errNoEntityMetrics
	}
	ts, ok := rc.Nodes[req.NodeName].Metrics[req.MetricName]
	if !ok {
		return nil, zeroTime, errNoSuchMetric
	}

	res := (*ts).Hour.Get(req.Start, req.End)
	return res, rc.timestamp, nil
}

// GetNamespaceMetric returns a metric of a namespace entity, along with the latest timestamp.
// GetNamespaceMetric receives as arguments the namespace, the metric name and a start time.
// GetNamespaceMetric returns a slice of TimePoints for that metric, with times starting AFTER the starting timestamp.
func (rc *realCluster) GetNamespaceMetric(req NamespaceMetricRequest) ([]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	if len(rc.Namespaces) == 0 {
		return nil, zeroTime, errModelEmpty
	}
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, zeroTime, errNoSuchNamespace
	}
	if len(ns.Metrics) == 0 {
		return nil, zeroTime, errNoEntityMetrics
	}
	ts, ok := ns.Metrics[req.MetricName]
	if !ok {
		return nil, zeroTime, errNoSuchMetric
	}

	res := (*ts).Hour.Get(req.Start, req.End)
	return res, rc.timestamp, nil
}

// GetPodMetric returns a metric of a Pod entity, along with the latest timestamp.
// GetPodMetric receives as arguments the namespace, the pod name, the metric name and a start time.
// GetPodMetric returns a slice of TimePoints for that metric, with times starting AFTER the starting timestamp.
func (rc *realCluster) GetPodMetric(req PodMetricRequest) ([]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	if len(rc.Namespaces) == 0 {
		return nil, zeroTime, errModelEmpty
	}
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, zeroTime, errNoSuchNamespace
	}
	pod, ok := ns.Pods[req.PodName]
	if !ok {
		return nil, zeroTime, errNoSuchPod
	}
	if len(pod.Metrics) == 0 {
		return nil, zeroTime, errNoEntityMetrics
	}
	ts, ok := pod.Metrics[req.MetricName]
	if !ok {
		return nil, zeroTime, errNoSuchMetric
	}

	res := (*ts).Hour.Get(req.Start, req.End)
	return res, rc.timestamp, nil
}

// GetBatchPodMetric returns metrics of a batch of Pod entities, along with the latest timestamp.
// GetBatchPodMetric receives as arguments the namespace, the pod names, the metric name and a start time.
// GetBatchPodMetric returns, for ach of the pods, slice of TimePoints for that metric, with times starting AFTER the starting timestamp
// (possibly empty if )
func (rc *realCluster) GetBatchPodMetric(req BatchPodRequest) ([][]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	if len(rc.Namespaces) == 0 {
		return nil, zeroTime, fmt.Errorf("the model is not populated yet")
	}
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, zeroTime, fmt.Errorf("the specified namespace is not present in the cluster")
	}
	result := make([][]statstore.TimePoint, len(req.PodNames))
	for i, podName := range req.PodNames {
		pod, ok := ns.Pods[podName]
		if !ok {
			result[i] = []statstore.TimePoint{}
			continue
		}
		if len(pod.Metrics) == 0 {
			result[i] = []statstore.TimePoint{}
			continue
		}
		ts, ok := pod.Metrics[req.MetricName]
		if !ok {
			result[i] = []statstore.TimePoint{}
			continue
		}
		res := (*ts).Hour.Get(req.Start, req.End)
		result[i] = res
	}
	return result, rc.timestamp, nil
}

// GetPodContainerMetric returns a metric of a container entity that belongs in a Pod, along with the latest timestamp.
// GetPodContainerMetric receives as arguments the namespace, the pod name, the container name, the metric name and a start time.
// GetPodContainerMetric returns a slice of TimePoints for that metric, with times starting AFTER the starting timestamp.
func (rc *realCluster) GetPodContainerMetric(req PodContainerMetricRequest) ([]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	if len(rc.Namespaces) == 0 {
		return nil, zeroTime, errModelEmpty
	}
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, zeroTime, errNoSuchNamespace
	}
	pod, ok := ns.Pods[req.PodName]
	if !ok {
		return nil, zeroTime, errNoSuchPod
	}
	ctr, ok := pod.Containers[req.ContainerName]
	if !ok {
		return nil, zeroTime, errNoSuchContainer
	}
	ts, ok := ctr.Metrics[req.MetricName]
	if !ok {
		return nil, zeroTime, errNoSuchMetric
	}

	res := (*ts).Hour.Get(req.Start, req.End)
	return res, rc.timestamp, nil
}

// GetFreeContainerMetric returns a metric of a free container entity, along with the latest timestamp.
// GetFreeContainerMetric receives as arguments the host name, the container name, the metric name and a start time.
// GetFreeContainerMetric returns a slice of TimePoints for that metric, with times starting AFTER the starting timestamp.
func (rc *realCluster) GetFreeContainerMetric(req FreeContainerMetricRequest) ([]statstore.TimePoint, time.Time, error) {
	var zeroTime time.Time
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	if len(rc.Nodes) == 0 {
		return nil, zeroTime, errModelEmpty
	}
	node, ok := rc.Nodes[req.NodeName]
	if !ok {
		return nil, zeroTime, errInvalidNode
	}
	ctr, ok := node.FreeContainers[req.ContainerName]
	if !ok {
		return nil, zeroTime, errNoSuchContainer
	}
	ts, ok := ctr.Metrics[req.MetricName]
	if !ok {
		return nil, zeroTime, errNoSuchMetric
	}

	res := (*ts).Hour.Get(req.Start, req.End)
	return res, rc.timestamp, nil
}

// makeEntityList creates an EntityListEntry from a map of metrics.
func makeEntityListEntry(name string, entities map[string]*daystore.DayStore) EntityListEntry {
	newListEntry := EntityListEntry{}
	cpu, ok := entities[cpuUsage]
	if !ok {
		newListEntry.CPUUsage = uint64(0)
	} else {
		lastHourCPU, err := cpu.Hour.Last()
		if err != nil {
			newListEntry.CPUUsage = uint64(0)
		} else {
			newListEntry.CPUUsage = lastHourCPU.Value
		}
	}

	mem, ok := entities[memWorking]
	if !ok {
		newListEntry.MemUsage = uint64(0)
	} else {
		lastHourMem, err := mem.Hour.Last()
		if err != nil {
			newListEntry.MemUsage = uint64(0)
		} else {
			newListEntry.MemUsage = lastHourMem.Value
		}
	}
	newListEntry.Name = name

	return newListEntry
}

// GetNodes returns a slice of EntityListEntry for all the nodes that are available on the cluster.
func (rc *realCluster) GetNodes() []EntityListEntry {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]EntityListEntry, 0)
	for key, val := range rc.Nodes {
		newEntity := makeEntityListEntry(key, val.Metrics)
		// Ignore entities with no name populated (errors)
		if newEntity.Name == "" {
			continue
		}
		res = append(res, newEntity)
	}
	return res
}

// findPodNamespace finds the namespace name that a given PodInfo belongs to
// assumes cluster lock is taken by the caller.
func (rc *realCluster) findPodNamespace(target *PodInfo) (string, error) {
	for namespace, nsref := range rc.Namespaces {
		for _, pod := range nsref.Pods {
			if pod == target {
				return namespace, nil
			}
		}
	}
	return "", fmt.Errorf("the specified pod does not belong under a namespace")
}

// GetNodePods returns the names and latest usage values of all the pods
// under a specific node.
func (rc *realCluster) GetNodePods(hostname string) []EntityListEntry {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]EntityListEntry, 0)
	noderef, ok := rc.Nodes[hostname]
	if !ok {
		return res
	}

	for podname, val := range noderef.Pods {
		// Set the Pod name as <namespace> / <podname>
		namespace, err := rc.findPodNamespace(val)
		if err != nil {
			break
		}
		newEntity := makeEntityListEntry(namespace+"/"+podname, val.Metrics)
		if newEntity.Name == "" {
			continue
		}
		res = append(res, newEntity)
	}
	fmt.Println(res)
	return res
}

// GetNamespaces returns the names and latest usage values of all the namespaces
// that are available in the model.
func (rc *realCluster) GetNamespaces() []EntityListEntry {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]EntityListEntry, 0)
	for key, val := range rc.Namespaces {
		newEntity := makeEntityListEntry(key, val.Metrics)
		if newEntity.Name == "" {
			continue
		}
		res = append(res, newEntity)
	}
	return res
}

// GetPods returns the names and latest usage values of all the pods that are
// available in the model under a specific namespace.
func (rc *realCluster) GetPods(namespace string) []EntityListEntry {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]EntityListEntry, 0)
	ns, ok := rc.Namespaces[namespace]
	if !ok {
		return res
	}

	for key, val := range ns.Pods {
		newEntity := makeEntityListEntry(key, val.Metrics)
		if newEntity.Name == "" {
			continue
		}
		res = append(res, newEntity)
	}
	return res
}

// GetPodContainers returns the names and latest usage values of all the containers
// that are available in the model under a specific namespace and pod.
func (rc *realCluster) GetPodContainers(namespace string, pod string) []EntityListEntry {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]EntityListEntry, 0)
	ns, ok := rc.Namespaces[namespace]
	if !ok {
		return res
	}

	podref, ok := ns.Pods[pod]
	if !ok {
		return res
	}
	for key, val := range podref.Containers {
		newEntity := makeEntityListEntry(key, val.Metrics)
		if newEntity.Name == "" {
			continue
		}
		res = append(res, newEntity)
	}
	return res
}

// GetFreeContainers returns the names and latest usage values of all the containers
//that are available in the model under a specific node.
func (rc *realCluster) GetFreeContainers(node string) []EntityListEntry {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]EntityListEntry, 0)
	noderef, ok := rc.Nodes[node]
	if !ok {
		return res
	}

	for key, val := range noderef.FreeContainers {
		newEntity := makeEntityListEntry(key, val.Metrics)
		if newEntity.Name == "" {
			continue
		}
		res = append(res, newEntity)
	}
	return res
}

// GetAvailableMetrics returns the names of all metrics that are available on the cluster.
// Due to metric propagation, all entities of the cluster have the same metrics.
func (rc *realCluster) GetAvailableMetrics() []string {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

	res := make([]string, 0)
	for key := range rc.Metrics {
		res = append(res, key)
	}
	return res
}

func (rc *realCluster) uptime(infotype *InfoType) time.Duration {
	return rc.timestamp.Sub(infotype.Creation)
}

// getClusterStats extracts the derived stats and uptime for the Cluster entity.
func (rc *realCluster) GetClusterStats() (map[string]StatBundle, time.Duration, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	return getStats(rc.InfoType), rc.uptime(&rc.InfoType), nil
}

// getNodeStats extracts the derived stats and uptime for a Node entity.
func (rc *realCluster) GetNodeStats(req NodeRequest) (map[string]StatBundle, time.Duration, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	node, ok := rc.Nodes[req.NodeName]
	if !ok {
		return nil, time.Duration(0), errInvalidNode
	}

	return getStats(node.InfoType), rc.uptime(&node.InfoType), nil
}

// getNamespaceStats extracts the derived stats and uptime for a Namespace entity.
func (rc *realCluster) GetNamespaceStats(req NamespaceRequest) (map[string]StatBundle, time.Duration, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, time.Duration(0), errNoSuchNamespace
	}

	return getStats(ns.InfoType), rc.uptime(&ns.InfoType), nil
}

// getPodStats extracts the derived stats and uptime for a Pod entity.
func (rc *realCluster) GetPodStats(req PodRequest) (map[string]StatBundle, time.Duration, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, time.Duration(0), errNoSuchNamespace
	}

	pod, ok := ns.Pods[req.PodName]
	if !ok {
		return nil, time.Duration(0), errNoSuchPod
	}

	return getStats(pod.InfoType), rc.uptime(&pod.InfoType), nil
}

// getPodContainerStats extracts the derived stats and uptime for a Pod Container entity.
func (rc *realCluster) GetPodContainerStats(req PodContainerRequest) (map[string]StatBundle, time.Duration, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	ns, ok := rc.Namespaces[req.NamespaceName]
	if !ok {
		return nil, time.Duration(0), errNoSuchNamespace
	}

	pod, ok := ns.Pods[req.PodName]
	if !ok {
		return nil, time.Duration(0), errNoSuchPod
	}

	ctr, ok := pod.Containers[req.ContainerName]
	if !ok {
		return nil, time.Duration(0), errNoSuchContainer
	}

	return getStats(ctr.InfoType), rc.uptime(&ctr.InfoType), nil
}

// getFreeContainerStats extracts the derived stats and uptime for a Pod Container entity.
func (rc *realCluster) GetFreeContainerStats(req FreeContainerRequest) (map[string]StatBundle, time.Duration, error) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	node, ok := rc.Nodes[req.NodeName]
	if !ok {
		return nil, time.Duration(0), errInvalidNode
	}

	ctr, ok := node.FreeContainers[req.ContainerName]
	if !ok {
		return nil, time.Duration(0), errNoSuchContainer
	}

	return getStats(ctr.InfoType), rc.uptime(&ctr.InfoType), nil
}
