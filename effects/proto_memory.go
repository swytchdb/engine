/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package effects

import (
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/swytchdb/engine/memsize"
)

// Protobuf snapshots share immutable submessages and value buffers with older
// effects. Track allocation identity, not serialized length or process heap.
// Both resident allocations and the ledger's map capacity contribute to bytes.
// Messages must remain immutable while resident.
type protoMemory struct {
	shards [64]allocationShard
}

type allocationShard struct {
	mu          sync.Mutex
	allocations map[uintptr]allocationOwners
	bytes       atomic.Int64
	highWater   int
	capacity    atomic.Int64
}

type allocationOwners struct {
	refs  int
	bytes int64
}

// Generated message layouts do not change. Cache the reference-bearing fields
// so each insertion avoids rediscovering scalar and protobuf runtime fields.
var messageMemoryFields sync.Map // reflect.Type -> []int

var allocationScratch = sync.Pool{New: func() any { return make(map[uintptr]int64) }}

func referenceFields(typ reflect.Type) []int {
	if fields, ok := messageMemoryFields.Load(typ); ok {
		return fields.([]int)
	}

	fields := make([]int, 0, typ.NumField())
	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() && field.Name != "unknownFields" {
			continue
		}

		kind := field.Type.Kind()
		if kind == reflect.Pointer || kind == reflect.Interface || kind == reflect.Slice || kind == reflect.Map || kind == reflect.String || kind == reflect.Struct || kind == reflect.Array {
			fields = append(fields, i)
		}
	}

	actual, _ := messageMemoryFields.LoadOrStore(typ, fields)

	return actual.([]int)
}

func (m *protoMemory) change(message any, delta int) {
	allocations := allocationScratch.Get().(map[uintptr]int64)
	collectAllocations(reflect.ValueOf(message), allocations)

	for address, size := range allocations {
		shard := &m.shards[((address>>4)^(address>>12))%uintptr(len(m.shards))]
		shard.mu.Lock()
		if shard.allocations == nil {
			shard.allocations = make(map[uintptr]allocationOwners)
		}

		owner := shard.allocations[address]
		if delta > 0 && size > owner.bytes {
			shard.bytes.Add(size - owner.bytes)
			owner.bytes = size
		}

		owner.refs += delta
		if owner.refs == 0 {
			shard.bytes.Add(-owner.bytes)
			delete(shard.allocations, address)
		} else {
			shard.allocations[address] = owner
		}

		if size := len(shard.allocations); size == 0 {
			shard.allocations = nil
			shard.highWater = 0
			shard.capacity.Store(0)
		} else if size > shard.highWater {
			shard.highWater = size
			shard.capacity.Store(memsize.Map(size, 24))
		} else if size*4 < shard.highWater {
			shrunk := make(map[uintptr]allocationOwners, size)
			maps.Copy(shrunk, shard.allocations)

			shard.allocations = shrunk
			shard.highWater = size
			shard.capacity.Store(memsize.Map(size, 24))
		}
		shard.mu.Unlock()
	}

	if len(allocations) <= 1024 {
		clear(allocations)
		allocationScratch.Put(allocations)
	}
}

func (m *protoMemory) residentBytes() int64 {
	var bytes int64
	for i := range m.shards {
		bytes += m.shards[i].bytes.Load() + m.shards[i].capacity.Load()
	}

	return bytes
}

// Walk generated protobuf structs, including oneof wrappers and unknown wire
// fields. Runtime message state and scalar fields are already in struct sizes.
// Capacity estimates are conservative; overlapping subslices with different
// starting addresses can be overestimated. Identical shared buffers count once.
func collectAllocations(v reflect.Value, allocations map[uintptr]int64) {
	if !v.IsValid() {
		return
	}

	add := func(address uintptr, size int64) bool {
		if address == 0 || size == 0 {
			return false
		}

		previous, seen := allocations[address]
		allocations[address] = max(previous, memsize.Alloc(size))

		return !seen
	}

	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			collectAllocations(v.Elem(), allocations)
		}
	case reflect.Pointer:
		if !v.IsNil() && add(v.Pointer(), int64(v.Type().Elem().Size())) {
			collectAllocations(v.Elem(), allocations)
		}
	case reflect.Struct:
		for _, i := range referenceFields(v.Type()) {
			collectAllocations(v.Field(i), allocations)
		}
	case reflect.Slice:
		if !v.IsNil() {
			add(v.Pointer(), int64(v.Cap())*int64(v.Type().Elem().Size()))
		}
		// Two fields can expose different lengths of the same backing array.
		// Visit both views; pointer identity still deduplicates their elements.
		if v.Type().Elem().Kind() != reflect.Uint8 {
			for i := range v.Len() {
				collectAllocations(v.Index(i), allocations)
			}
		}
	case reflect.Map:
		if !v.IsNil() && add(uintptr(v.UnsafePointer()), memsize.Map(v.Len(), int64(v.Type().Key().Size()+v.Type().Elem().Size()))) {
			iter := v.MapRange()
			for iter.Next() {
				collectAllocations(iter.Key(), allocations)
				collectAllocations(iter.Value(), allocations)
			}
		}
	case reflect.String:
		if v.Len() > 0 {
			add(uintptr(unsafe.Pointer(unsafe.StringData(v.String()))), int64(v.Len()))
		}
	case reflect.Array:
		for i := range v.Len() {
			collectAllocations(v.Index(i), allocations)
		}
	case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.Chan, reflect.Func, reflect.UnsafePointer:
		// Numbers and other scalar fields have no separately owned allocation.
	}
}
