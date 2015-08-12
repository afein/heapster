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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/GoogleCloudPlatform/heapster/store"
)

// TestHalfDay tests all methods of a dayStore that has not rewinded.
func TestHalfDay(t *testing.T) {
	// epsilon: 10
	ds := newDayStore(10)
	assert := assert.New(t)
	now := time.Now().Truncate(time.Minute)

	// Invocations with nothing in the dayStore
	avg, err := ds.Average()
	assert.Error(err)
	assert.Equal(avg, 0)

	max, err := ds.Max()
	assert.Error(err)
	assert.Equal(max, 0)

	nf, err := ds.NinetyFifth()
	assert.Error(err)
	assert.Equal(nf, 0)

	// Put 8 Points, two within each hour.
	// The second point of each hour is ignored in further derivations,
	// as it is held in lastPut
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now,
		Value:     uint64(55),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(50 * time.Minute),
		Value:     uint64(100),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(time.Hour),
		Value:     uint64(1),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(time.Hour).Add(50 * time.Minute),
		Value:     uint64(100),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(2 * time.Hour),
		Value:     uint64(22),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(2 * time.Hour).Add(50 * time.Minute),
		Value:     uint64(100),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(3 * time.Hour),
		Value:     uint64(77),
	}))
	assert.NoError(ds.Put(store.TimePoint{
		Timestamp: now.Add(3 * time.Hour).Add(50 * time.Minute),
		Value:     uint64(100),
	}))

	// Invocations with nothing in the dayStore
	// Average: (60 + 10 + 30 + 80) / 4 = 180
	avg, err = ds.Average()
	assert.NoError(err)
	assert.Equal(avg, uint64(180))

	// Max: 80
	max, err = ds.Max()
	assert.NoError(err)
	assert.Equal(max, uint64(80))

	nf, err = ds.NinetyFifth()
	assert.NoError(err)
	assert.Equal(nf, uint64(80))
}
