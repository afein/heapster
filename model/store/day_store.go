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

package store

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/heapster/store"
)

func newDayStore(epsilon uint64) *dayStore {
	return &dayStore{
		window: NewWindow(23, 1),
		Hour:   NewStatStore(epsilon, time.Minute, 60, []float64{0.95}),
	}
}

type dayStore struct {
	sync.RWMutex

	// Hour is a statStore with data from the last one hour
	Hour *statStore

	window *MovingWindow

	// size is the number of items currently stored in the window
	size int

	lastFlush time.Time

	validCache bool

	cachedAverage     uint64
	cachedMax         uint64
	cachedNinetyFifth uint64
}

type hourEntry struct {
	average     uint64
	max         uint64
	ninetyFifth uint64
}

// Put stores a TimePoint into the Hour statStore, while checking whether it -
// is time to flush the last hour's stats in the window.
func (ds *dayStore) Put(tp store.TimePoint) error {
	ds.Lock()
	defer ds.Unlock()

	err := ds.Hour.Put(tp)
	if err != nil {
		return err
	}

	// Check if this is the first TimePoint ever, in which case flush in one hour.
	if ds.lastFlush.Equal(time.Time{}) {
		ds.lastFlush = tp.Timestamp
		return nil
	}

	// The new TimePoint is newer by at least one hour since the last flush
	if !tp.Timestamp.Add(-time.Hour).Before(ds.lastFlush) {
		avg, _ := ds.Hour.Average()
		max, _ := ds.Hour.Max()
		nf, _ := ds.Hour.Percentile(0.95)
		newEntry := hourEntry{
			average:     avg,
			max:         max,
			ninetyFifth: nf,
		}
		if ds.size < 23 {
			ds.size += 1
		}
		ds.window.PushBack(newEntry)
		ds.lastFlush = tp.Timestamp

	}
	return nil
}

// fillCache caches the average, max and 95th percentile of the dayStore.
func (ds *dayStore) fillCache() {
	// generate a slice of the window
	day := ds.window.Slice()

	// calculate the average value of the hourly averages
	// and the max value of all the hourly maxes
	sum, _ := ds.Hour.Average()
	curMax, _ := ds.Hour.Max()
	lp, _ := ds.Hour.Percentile(0.95)
	nf := []float64{float64(lp)}

	for _, he := range day {
		sum += he.average
		if he.max > curMax {
			curMax = he.max
		}
		nf = append(nf, float64(he.ninetyFifth))
	}
	ds.cachedAverage = sum / uint64(ds.size+1)
	ds.cachedMax = curMax

	// calculate the 95th percentile
	sort.Float64s(nf)
	pcIdx := int(math.Trunc(0.95 * float64(ds.size+1)))
	if pcIdx > (len(nf) - 1) {
		pcIdx = len(nf) - 1
	}
	ds.cachedNinetyFifth = uint64(nf[pcIdx])
	ds.validCache = true
}

// Average returns the average value of the hourly averages in the past day.
func (ds *dayStore) Average() (uint64, error) {
	ds.Lock()
	defer ds.Unlock()

	if ds.Hour.buffer.Front() == nil {
		return uint64(0), fmt.Errorf("the dayStore is empty")
	}

	if !ds.validCache {
		ds.fillCache()
	}

	return ds.cachedAverage, nil
}

// Max returns the maximum value of the hourly maxes in the past day.
func (ds *dayStore) Max() (uint64, error) {
	ds.Lock()
	defer ds.Unlock()

	if ds.Hour.buffer.Front() == nil {
		return uint64(0), fmt.Errorf("the dayStore is empty")
	}

	if !ds.validCache {
		ds.fillCache()
	}

	return ds.cachedMax, nil
}

// NinetyFifth returns the 95th percentile of the hourly 95th percentiles in the past day.
func (ds *dayStore) NinetyFifth() (uint64, error) {
	ds.Lock()
	defer ds.Unlock()

	if ds.Hour.buffer.Front() == nil {
		return uint64(0), fmt.Errorf("the dayStore is empty")
	}

	if !ds.validCache {
		ds.fillCache()
	}

	return ds.cachedNinetyFifth, nil
}
