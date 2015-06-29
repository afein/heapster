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

package v1

import (
	"net/http"
	"time"

	"github.com/GoogleCloudPlatform/heapster/manager"
	sinksApi "github.com/GoogleCloudPlatform/heapster/sinks/api/v1"
	"github.com/GoogleCloudPlatform/heapster/util"
	restful "github.com/emicklei/go-restful"
	"github.com/golang/glog"
)

type Api struct {
	manager manager.Manager
}

// Create a new Api to serve from the specified cache.
func NewApi(m manager.Manager) *Api {
	return &Api{
		manager: m,
	}
}

// Register the Api on the specified endpoint.
func (a *Api) Register(container *restful.Container) {
	ws := new(restful.WebService)
	ws.
		Path("/api/v1/metric-export").
		Doc("Exports the latest point for all Heapster metrics").
		Produces(restful.MIME_JSON)
	ws.Route(ws.GET("").
		Filter(compressionFilter).
		To(a.exportMetrics).
		Doc("export the latest data point for all metrics").
		Operation("exportMetrics").
		Writes([]*Timeseries{}))
	container.Add(ws)
	ws = new(restful.WebService)
	ws.Path("/api/v1/metric-export-schema").
		Doc("Schema for metrics exported by heapster").
		Produces(restful.MIME_JSON)
	ws.Route(ws.GET("").
		To(a.exportMetricsSchema).
		Doc("export the schema for all metrics").
		Operation("exportmetricsSchema").
		Writes(TimeseriesSchema{}))
	container.Add(ws)

	a.RegisterCluster(container)
}

// Register the Cluster API endpoints
func (a *Api) RegisterCluster(container *restful.Container) {
	// TODO(afein): show all metrics on /api/v1/cluster/
	ws := new(restful.WebService)
	ws.
		Path("/api/v1/").
		Doc("Exports all aggregated Cluster-level metrics since the specified start time").
		Consumes("*/*").
		Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/metrics").
		To(a.availableMetrics).
		Filter(compressionFilter).
		Doc("Show which metrics are available").
		Operation("clusterMetrics"))

	ws.Route(ws.GET("/cluster/{metric-name}/{start}").
		To(a.clusterMetrics).
		Filter(compressionFilter).
		Doc("export all aggregated cluster-level metrics").
		Operation("clusterMetrics").
		Param(ws.PathParameter("metric-name", "The name of the requested metric").DataType("string")).
		Param(ws.PathParameter("start", "Start time for requested metrics").DataType("string")).
		Writes(MetricResult{}))
	container.Add(ws)
}

func compressionFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// wrap responseWriter into a compressing one
	compress, err := restful.NewCompressingResponseWriter(resp.ResponseWriter, restful.ENCODING_GZIP)
	if err != nil {
		glog.Warningf("Failed to create CompressingResponseWriter for request %q: %v", req.Request.URL, err)
		return
	}
	resp.ResponseWriter = compress
	defer compress.Close()
	chain.ProcessFilter(req, resp)
}

// Labels used by the target schema. A target schema uniquely identifies a container.
var targetLabelNames = map[string]struct{}{
	sinksApi.LabelPodId.Key:           struct{}{},
	sinksApi.LabelPodName.Key:         struct{}{},
	sinksApi.LabelPodNamespace.Key:    struct{}{},
	sinksApi.LabelContainerName.Key:   struct{}{},
	sinksApi.LabelLabels.Key:          struct{}{},
	sinksApi.LabelHostname.Key:        struct{}{},
	sinksApi.LabelHostID.Key:          struct{}{},
	sinksApi.LabelPodNamespaceUID.Key: struct{}{},
}

// Separates target schema labels from other labels.
func separateLabels(labels map[string]string) (map[string]string, map[string]string) {
	targetLabels := make(map[string]string, len(targetLabelNames))
	otherLabels := make(map[string]string, len(labels)-len(targetLabels))
	for label, _ := range labels {
		// Ignore blank labels.
		if label == "" {
			continue
		}

		if _, ok := targetLabelNames[label]; ok {
			targetLabels[label] = labels[label]
		} else {
			otherLabels[label] = labels[label]
		}
	}

	return targetLabels, otherLabels
}

func (a *Api) exportMetricsSchema(request *restful.Request, response *restful.Response) {
	result := TimeseriesSchema{}
	for _, label := range sinksApi.CommonLabels() {
		result.CommonLabels = append(result.CommonLabels, LabelDescriptor{
			Key:         label.Key,
			Description: label.Description,
		})
	}
	for _, label := range sinksApi.PodLabels() {
		result.PodLabels = append(result.PodLabels, LabelDescriptor{
			Key:         label.Key,
			Description: label.Description,
		})
	}

	for _, metric := range sinksApi.SupportedStatMetrics() {
		md := MetricDescriptor{
			Name:        metric.Name,
			Description: metric.Description,
			Type:        metric.Type.String(),
			ValueType:   metric.ValueType.String(),
			Units:       metric.Units.String(),
		}
		for _, label := range metric.Labels {
			md.Labels = append(md.Labels, LabelDescriptor{
				Key:         label.Key,
				Description: label.Description,
			})
		}
		result.Metrics = append(result.Metrics, md)
	}
	response.WriteEntity(result)
}

// availableMetrics returns a list of available metric names.
// These metric names can be used to extract metrics from the various resources
func (a *Api) availableMetrics(request *restful.Request, response *restful.Response) {
	cluster := a.manager.GetCluster()
	result := cluster.GetAvailableMetrics()
	response.WriteEntity(result)
}

// clusterMetrics returns a metric timeseries for the cluster resource
func (a *Api) clusterMetrics(request *restful.Request, response *restful.Response) {
	var err error
	cluster := a.manager.GetCluster()
	// Get metric name
	metric_name := request.PathParameter("metric-name")

	// Get start time, parse as time.Time
	query_param := request.PathParameter("start")
	req_stamp := time.Time{}
	if query_param != "" {
		time_format := "2013-02-29T22:06:00Z"
		req_stamp, err = time.Parse(time_format, query_param)
		if err != nil {
			// Timestamp parameter cannot be parsed
			response.WriteError(http.StatusInternalServerError, err)
			glog.Errorf("timestamp argument cannot be parsed: %s", err)
			return
		}
	}

	timeseries, new_stamp, err := cluster.GetClusterMetric(metric_name, req_stamp)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		glog.Errorf("unable to get cluster metric: %s", err)
		return
	}

	// Convert each store.TimePoint to a MetricPoint
	res_metrics := []MetricPoint{}
	for _, metric := range timeseries {
		newMP := MetricPoint{
			Timestamp: metric.Timestamp,
			Value:     metric.Value.(uint64),
		}
		res_metrics = append(res_metrics, newMP)
	}

	result := MetricResult{
		Metrics:         res_metrics,
		LatestTimestamp: new_stamp,
	}
	response.WriteEntity(result)
}

func (a *Api) exportMetrics(request *restful.Request, response *restful.Response) {
	points, err := a.manager.ExportMetrics()
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// Group points by target labels.
	timeseriesForTargetLabels := map[string]*Timeseries{}
	for _, point := range points {
		targetLabels, otherLabels := separateLabels(point.Labels)
		labelsStr := util.LabelsToString(targetLabels, ",")

		// Add timeseries if it does not exist.
		timeseries, ok := timeseriesForTargetLabels[labelsStr]
		if !ok {
			timeseries = &Timeseries{
				Metrics: map[string][]Point{},
				Labels:  targetLabels,
			}
			timeseriesForTargetLabels[labelsStr] = timeseries
		}

		// Add point to this timeseries
		timeseries.Metrics[point.Name] = append(timeseries.Metrics[point.Name], Point{
			Start:  point.Start,
			End:    point.End,
			Labels: otherLabels,
			Value:  point.Value,
		})
	}

	// Turn into a slice.
	timeseries := make([]*Timeseries, 0, len(timeseriesForTargetLabels))
	for _, val := range timeseriesForTargetLabels {
		timeseries = append(timeseries, val)
	}

	response.WriteEntity(timeseries)
}
