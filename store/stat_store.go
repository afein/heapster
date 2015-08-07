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
	"sync"
	"time"

	"github.com/google/cadvisor/summary"
)

const (
	Fiftieth    float64 = 0.50
	Ninetieth   float64 = 0.90
	NinetyFifth float64 = 0.95
)

// StatStore is a TimeStore that can also fetch stats over its own data.
// It assumes that the underlying TimeStore uses TimePoint values of type
// uint64.
type StatStore interface {
	TimeStore

	// Last returns the last TimePoint.
	Last() *TimePoint
	// GetAverage gets the average value of all TimePoints.
	Average() uint64
	// GetMax gets the max value of the all TimePoints.
	Max() uint64
	// Percentile gets the specified Nth percentile of all TimePoints.
	Percentile(n float64) uint64
}

type statStore struct {
	sync.Mutex
	ts TimeStore

	validCache        bool
	cachedAverage     uint64
	cachedMax         uint64
	cachedFiftieth    uint64
	cachedNinetieth   uint64
	cachedNinetyFifth uint64
}

func (s *statStore) Put(tp TimePoint) error {
	s.Lock()
	defer s.Unlock()
	s.validCache = false
	return s.ts.Put(tp)
}

func (s *statStore) Get(start, end time.Time) []TimePoint {
	return s.ts.Get(start, end)
}

func (s *statStore) Delete(start, end time.Time) error {
	s.Lock()
	defer s.Unlock()
	s.validCache = false
	return s.ts.Delete(start, end)
}

func (s *statStore) getAll() []TimePoint {
	zeroTime := time.Time{}
	return s.ts.Get(zeroTime, zeroTime)
}

func (s *statStore) Last() *TimePoint {
	all := s.getAll()
	if len(all) < 1 {
		return nil
	}
	// To not give the impression that this allows the caller to change
	// the last value in the underlying data structure, return the address
	// of a copy
	last := all[len(all)-1]
	return &last
}

type Uint64Slice []uint64

func (a Uint64Slice) Len() int           { return len(a) }
func (a Uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

// Get percentile of the provided samples. Round to integer.
func (self Uint64Slice) GetPercentile(d float64) uint64 {
	if d < 0.0 || d > 1.0 {
		return 0
	}
	count := self.Len()
	if count == 0 {
		return 0
	}
	sort.Sort(self)
	n := float64(d * (float64(count) + 1))
	idx, frac := math.Modf(n)
	index := int(idx)
	percentile := float64(self[index-1])
	if index > 1 && index < count {
		percentile += frac * float64(self[index]-self[index-1])
	}
	return uint64(percentile)
}

func (s *statStore) fillCache() {
	if s.validCache {
		return
	}

	s.validCache = true
	s.cachedAverage = 0
	s.cachedMax = 0
	s.cachedFiftieth = 0
	s.cachedNinetieth = 0
	s.cachedNinetyFifth = 0

	all := s.getAll()
	if len(all) < 1 {
		return
	}

	inc := make(summary.Uint64Slice, 0, len(all))
	for _, tp := range all {
		inc = append(inc, tp.Value.(uint64))
	}
	acc := uint64(0)
	for _, u := range inc {
		acc += u
	}
	s.cachedAverage = uint64(float64(acc) / float64(len(inc)))
	s.cachedFiftieth = inc.GetPercentile(Fiftieth)
	s.cachedNinetieth = inc.GetPercentile(Ninetieth)
	s.cachedNinetyFifth = inc.GetPercentile(NinetyFifth)

	// inc is sorted in ascending order by GetPercentile
	// TODO(afein || mvdan): sort explicitly only once
	s.cachedMax = inc[len(inc)-1]
}

func (s *statStore) Average() uint64 {
	s.Lock()
	defer s.Unlock()
	s.fillCache()
	return s.cachedAverage
}

func (s *statStore) Max() uint64 {
	s.Lock()
	defer s.Unlock()
	s.fillCache()
	return s.cachedMax
}

func (s *statStore) Percentile(n float64) uint64 {
	s.Lock()
	defer s.Unlock()
	s.fillCache()
	switch n {
	case Fiftieth:
		return s.cachedFiftieth
	case Ninetieth:
		return s.cachedNinetieth
	case NinetyFifth:
		return s.cachedNinetyFifth
	}
}

func NewStatStore(store TimeStore) StatStore {
	return &statStore{
		ts: store,
	}
}
