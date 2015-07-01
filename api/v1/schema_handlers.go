package v1

import (
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
