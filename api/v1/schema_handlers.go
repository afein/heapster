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

	restful "github.com/emicklei/go-restful"
	"github.com/golang/glog"
)

// Register the Cluster API endpoints
func (a *Api) RegisterCluster(container *restful.Container) {
	ws := new(restful.WebService)
	ws.
		Path("/api/v1/schema").
		Doc("Exports all aggregated Cluster-level metrics since the specified start time").
		Consumes("*/*").
		Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/metrics").
		To(a.availableMetrics).
		Filter(compressionFilter).
		Doc("Show which metrics are available").
		Operation("clusterMetrics"))

	ws.Route(ws.GET("/cluster/{metric-name}").
		To(a.clusterMetrics).
		Filter(compressionFilter).
		Doc("export all aggregated cluster-level metrics").
		Operation("clusterMetrics").
		Param(ws.PathParameter("metric-name", "The name of the requested metric").DataType("string")).
		Param(ws.QueryParameter("start", "Start time for requested metrics").DataType("string")).
		Writes(MetricResult{}))
	container.Add(ws)
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
	query_param := request.QueryParameter("start")
	req_stamp := time.Time{}
	if query_param != "" {
		req_stamp, err = time.Parse(time.RFC3339, query_param)
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
