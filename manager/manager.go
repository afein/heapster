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

package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/GoogleCloudPlatform/heapster/model"
	"github.com/GoogleCloudPlatform/heapster/sinks"
	sink_api "github.com/GoogleCloudPlatform/heapster/sinks/api/v1"
	"github.com/GoogleCloudPlatform/heapster/sinks/cache"
	source_api "github.com/GoogleCloudPlatform/heapster/sources/api"
	"github.com/GoogleCloudPlatform/heapster/store"
)

// Manager provides an interface to control the core of heapster.
// Implementations are not required to be thread safe.
type Manager interface {
	// Housekeep collects data from all the configured sources and
	// stores the data to all the configured sinks.
	Housekeep()

	// HousekeepModel performs housekeeping for the Model entity
	HousekeepModel()

	// Export the latest data point of all metrics.
	ExportMetrics() ([]*sink_api.Point, error)

	// Set the sinks to use
	SetSinkUris(Uris) error

	// Get the sinks currently in use
	SinkUris() Uris

	// Get a reference to the cluster entity of the model, if it exists.
	GetCluster() model.Cluster
}

type realManager struct {
	sources     []source_api.Source
	cache       cache.Cache
	model       model.Cluster
	sinkManager sinks.ExternalSinkManager
	sinkUris    Uris
	lastSync    time.Time
	resolution  time.Duration
	align       bool
	decoder     sink_api.Decoder
}

type syncData struct {
	data  source_api.AggregateData
	mutex sync.Mutex
}

func NewManager(sources []source_api.Source, sinkManager sinks.ExternalSinkManager, res, bufferDuration time.Duration, useModel bool, modelRes time.Duration, align bool) (Manager, error) {
	var newCluster model.Cluster = nil
	if useModel {
		// TimeStore and DayStore constructors passed to the model.
		tsConstructor := func() store.TimeStore {
			// TODO(afein): determine default analogy of cache duration to Timestore durations.
			return store.NewGCStore(store.NewCMAStore(), time.Hour)
		}
		dayConstructor := func() store.DayStore {
			return store.NewDayStore(20 * time.Minute) // TODO(afein): fine-tune this value
		}

		newCluster = model.NewCluster(dayConstructor, tsConstructor, modelRes)
	}
	firstSync := time.Now()
	if align {
		firstSync = firstSync.Truncate(res).Add(res)
	}
	return &realManager{
		sources:     sources,
		sinkManager: sinkManager,
		cache:       cache.NewCache(bufferDuration),
		model:       newCluster,
		lastSync:    firstSync,
		resolution:  res,
		align:       align,
		decoder:     sink_api.NewDecoder(),
	}, nil
}

func (rm *realManager) GetCluster() model.Cluster {
	return rm.model
}

func (rm *realManager) scrapeSource(s source_api.Source, start, end time.Time, sd *syncData, errChan chan<- error) {
	glog.V(2).Infof("attempting to get data from source %q", s.Name())
	data, err := s.GetInfo(start, end, rm.resolution, rm.align)
	if err != nil {
		errChan <- fmt.Errorf("failed to get information from source %q - %v", s.Name(), err)
		return
	}
	sd.mutex.Lock()
	defer sd.mutex.Unlock()
	sd.data.Merge(&data)
	errChan <- nil
}

// HousekeepModel periodically populates the manager model from the manager cache.
func (rm *realManager) HousekeepModel() {
	if rm.model != nil {
		if err := rm.model.Update(rm.cache); err != nil {
			glog.V(1).Infof("Model housekeeping returned error: %s", err.Error())
		}
	}
}

func (rm *realManager) Housekeep() {
	errChan := make(chan error, len(rm.sources))
	var sd syncData
	start := rm.lastSync
	end := time.Now()
	if rm.align {
		end = end.Truncate(rm.resolution)
		if start.After(end) {
			return
		}
	}
	rm.lastSync = end
	glog.V(2).Infof("starting to scrape data from sources start:%v end:%v", start, end)
	for idx := range rm.sources {
		s := rm.sources[idx]
		go rm.scrapeSource(s, start, end, &sd, errChan)
	}
	var errors []string
	for i := 0; i < len(rm.sources); i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err.Error())
		}
	}
	glog.V(2).Infof("completed scraping data from sources. Errors: %v", errors)
	if err := rm.cache.StorePods(sd.data.Pods); err != nil {
		errors = append(errors, err.Error())
	}
	if err := rm.cache.StoreContainers(sd.data.Machine); err != nil {
		errors = append(errors, err.Error())
	}
	if err := rm.cache.StoreContainers(sd.data.Containers); err != nil {
		errors = append(errors, err.Error())
	}
	if err := rm.sinkManager.Store(sd.data); err != nil {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		glog.V(1).Infof("housekeeping resulted in following errors: %v", errors)
	}
}

func (rm *realManager) ExportMetrics() ([]*sink_api.Point, error) {
	var zero time.Time

	// Get all pods as points.
	pods := trimStatsForPods(rm.cache.GetPods(zero, zero))
	timeseries, err := rm.decoder.TimeseriesFromPods(pods)
	if err != nil {
		return []*sink_api.Point{}, err
	}
	points := make([]*sink_api.Point, 0, len(timeseries))
	points = appendPoints(points, timeseries)

	// Get all nodes as points.
	containers := trimStatsForContainers(rm.cache.GetNodes(zero, zero))
	timeseries, err = rm.decoder.TimeseriesFromContainers(containers)
	if err != nil {
		return []*sink_api.Point{}, err
	}
	points = appendPoints(points, timeseries)

	// Get all free containers as points.
	containers = trimStatsForContainers(rm.cache.GetFreeContainers(zero, zero))
	timeseries, err = rm.decoder.TimeseriesFromContainers(containers)
	if err != nil {
		return []*sink_api.Point{}, err
	}
	points = appendPoints(points, timeseries)

	return points, nil
}

// Extract the points from the specified timeseries and append them to output.
func appendPoints(output []*sink_api.Point, toExtract []sink_api.Timeseries) []*sink_api.Point {
	for i := range toExtract {
		output = append(output, toExtract[i].Point)
	}
	return output
}

// Only keep latest stats for the specified pods
func trimStatsForPods(pods []*cache.PodElement) []*cache.PodElement {
	for _, pod := range pods {
		trimStatsForContainers(pod.Containers)
	}
	return pods
}

// Only keep latest stats for the specified containers
func trimStatsForContainers(containers []*cache.ContainerElement) []*cache.ContainerElement {
	for _, cont := range containers {
		onlyKeepLatestStat(cont)
	}
	return containers
}

// Only keep the latest stats data point.
func onlyKeepLatestStat(cont *cache.ContainerElement) {
	if len(cont.Metrics) > 1 {
		cont.Metrics = cont.Metrics[0:1]
	}
}

func (rm *realManager) SetSinkUris(sinkUris Uris) error {
	sinks, err := newSinks(sinkUris)
	if err != nil {
		return err
	}
	if err := rm.sinkManager.SetSinks(sinks); err != nil {
		return err
	}
	rm.sinkUris = sinkUris
	return nil
}

func (rm *realManager) SinkUris() Uris {
	return rm.sinkUris
}
