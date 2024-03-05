// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// simpleBatchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type batchStream []chunk.Batch

// reset, hasNext, next, atTime etc are all inlined in go1.11.

func (bs *batchStream) reset() {
	for i := range *bs {
		(*bs)[i].Index = 0
	}
}

func (bs *batchStream) hasNext() chunkenc.ValueType {
	if len(*bs) > 0 {
		return (*bs)[0].ValueType
	}
	return chunkenc.ValNone
}

func (bs *batchStream) next() {
	(*bs)[0].Index++
	if (*bs)[0].Index >= (*bs)[0].Length {
		*bs = (*bs)[1:]
	}
}

func (bs *batchStream) atTime() int64 {
	return (*bs)[0].Timestamps[(*bs)[0].Index]
}

func (bs *batchStream) at() (int64, float64) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.Values[b.Index]
}

func (bs *batchStream) atHistogram() (int64, unsafe.Pointer) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.PointerValues[b.Index]
}

func (bs *batchStream) atFloatHistogram() (int64, unsafe.Pointer) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.PointerValues[b.Index]
}

// mergeStreams merges streams of Batches of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
// hPool and fhPool are pools of pointers where the discarded histogram.Histogram and histogram.FloatHistogram objects from left and right streams
// will be placed, so that they can be reused. If hPool and fhPool are nil, the discarded histogram.Histogram and histogram.FloatHistogram objects
// will not be reused.
func mergeStreams(left, right, result batchStream, size int, hPool *zeropool.Pool[*histogram.Histogram], fhPool *zeropool.Pool[*histogram.FloatHistogram]) batchStream {

	// Reset the Index and Length of existing batches.
	for i := range result {
		result[i].Index = 0
		result[i].Length = 0
	}

	resultLen := 1 // Number of batches in the final result.
	b := &result[0]

	// Step to the next Batch in the result, create it if it does not exist
	nextBatch := func(valueType chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(result) {
			// It is possible that result can grow longer
			// then the one provided.
			result = append(result, chunk.Batch{})
		}
		b = &result[resultLen-1]
		b.ValueType = valueType
	}

	populate := func(s batchStream, valueType chunkenc.ValueType) {
		if b.Index == 0 {
			// Starting to write this Batch, it is safe to set the value type
			b.ValueType = valueType
		} else if b.Index == size || b.ValueType != valueType {
			// The batch reached its intended size or is of a different value type
			// Add another batch to the result and use it for further appending.
			nextBatch(valueType)
		}

		switch valueType {
		case chunkenc.ValFloat:
			b.Timestamps[b.Index], b.Values[b.Index] = s.at()
		case chunkenc.ValHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = s.atHistogram()
		case chunkenc.ValFloatHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = s.atFloatHistogram()
		}
		b.Index++
	}

	for lt, rt := left.hasNext(), right.hasNext(); lt != chunkenc.ValNone && rt != chunkenc.ValNone; lt, rt = left.hasNext(), right.hasNext() {
		t1, t2 := left.atTime(), right.atTime()
		if t1 < t2 {
			populate(left, lt)
			left.next()
		} else if t1 > t2 {
			populate(right, rt)
			right.next()
		} else {
			if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
				// Prefer histograms than floats. Take left side if both have histograms.
				populate(right, rt)
			} else {
				populate(left, lt)
				if rt == chunkenc.ValHistogram && hPool != nil {
					_, h := right.atHistogram()
					hPool.Put((*histogram.Histogram)(h))
				}
				if rt == chunkenc.ValFloatHistogram && fhPool != nil {
					_, fh := right.atFloatHistogram()
					fhPool.Put((*histogram.FloatHistogram)(fh))
				}
			}
			left.next()
			right.next()
		}
	}

	// This function adds all the samples from the provided
	// simpleBatchStream into the result in the same order.
	addToResult := func(bs batchStream) {
		for t := bs.hasNext(); t != chunkenc.ValNone; t = bs.hasNext() {
			populate(bs, t)
			bs.next()
		}
	}

	addToResult(left)
	addToResult(right)

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	// The provided 'result' slice might be bigger
	// than the actual result, hence return the subslice.
	result = result[:resultLen]
	result.reset()
	return result
}

// batchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type mergeableBatchStream struct {
	batches    []chunk.Batch
	batchesBuf []chunk.Batch

	hPool  *zeropool.Pool[*histogram.Histogram]
	fhPool *zeropool.Pool[*histogram.FloatHistogram]
}

func newBatchStream(size int, hPool *zeropool.Pool[*histogram.Histogram], fhPool *zeropool.Pool[*histogram.FloatHistogram]) *mergeableBatchStream {
	batches := make([]chunk.Batch, 0, size)
	batchesBuf := make([]chunk.Batch, size)
	return &mergeableBatchStream{
		batches:    batches,
		batchesBuf: batchesBuf,
		hPool:      hPool,
		fhPool:     fhPool,
	}
}

func (bs *mergeableBatchStream) putPointerValuesToThePool(batch *chunk.Batch) {
	if batch.ValueType == chunkenc.ValHistogram && bs.hPool != nil {
		for i := 0; i < batch.Length; i++ {
			bs.hPool.Put((*histogram.Histogram)(batch.PointerValues[i]))
		}
	} else if batch.ValueType == chunkenc.ValFloatHistogram && bs.fhPool != nil {
		for i := 0; i < batch.Length; i++ {
			bs.fhPool.Put((*histogram.FloatHistogram)(batch.PointerValues[i]))
		}
	}
}

func (bs *mergeableBatchStream) removeFirst() {
	bs.putPointerValuesToThePool(bs.curr())
	copy(bs.batches, bs.batches[1:])
	bs.batches = bs.batches[:len(bs.batches)-1]
}

func (bs *mergeableBatchStream) empty() {
	for i := range bs.batches {
		bs.putPointerValuesToThePool(&bs.batches[i])
	}
	bs.batches = bs.batches[:0]
}

func (bs *mergeableBatchStream) len() int {
	return len(bs.batches)
}

func (bs *mergeableBatchStream) reset() {
	for i := range bs.batches {
		bs.batches[i].Index = 0
	}
}

func (bs *mergeableBatchStream) hasNext() chunkenc.ValueType {
	if bs.len() > 0 {
		return bs.curr().ValueType
	}
	return chunkenc.ValNone
}

func (bs *mergeableBatchStream) next() {
	b := bs.curr()
	b.Index++
	if b.Index >= b.Length {
		bs.batches = bs.batches[1:]
	}
}

func (bs *mergeableBatchStream) curr() *chunk.Batch {
	return &bs.batches[0]
}

// merge merges this streams of chunk.Batch objects and the given chunk.Batch of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
func (bs *mergeableBatchStream) merge(batch *chunk.Batch, size int) {
	bs.batches = append(bs.batches[:0], bs.mergeStreams(batch, size)...)
	bs.reset()
}

func (bs *mergeableBatchStream) mergeStreams(batch *chunk.Batch, size int) []chunk.Batch {
	// Reset the Index and Length of existing batches.
	for i := range bs.batchesBuf {
		bs.batchesBuf[i].Index = 0
		bs.batchesBuf[i].Length = 0
	}

	resultLen := 1 // Number of batches in the final result.
	b := &bs.batchesBuf[0]

	// Step to the next Batch in the result, create it if it does not exist
	nextBatch := func(valueType chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(bs.batchesBuf) {
			// It is possible that result can grow longer
			// then the one provided.
			bs.batchesBuf = append(bs.batchesBuf, chunk.Batch{})
		}
		b = &bs.batchesBuf[resultLen-1]
		b.ValueType = valueType
	}

	populate := func(batch *chunk.Batch, valueType chunkenc.ValueType) {
		if b.Index == 0 {
			// Starting to write this Batch, it is safe to set the value type
			b.ValueType = valueType
		} else if b.Index == size || b.ValueType != valueType {
			// The batch reached its intended size or is of a different value type
			// Add another batch to the result and use it for further appending.
			nextBatch(valueType)
		}

		switch valueType {
		case chunkenc.ValFloat:
			b.Timestamps[b.Index], b.Values[b.Index] = batch.At()
		case chunkenc.ValHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = batch.AtHistogram()
		case chunkenc.ValFloatHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = batch.AtFloatHistogram()
		}
		b.Index++
	}

	for lt, rt := bs.hasNext(), batch.HasNext(); lt != chunkenc.ValNone && rt != chunkenc.ValNone; lt, rt = bs.hasNext(), batch.HasNext() {
		t1, t2 := bs.curr().AtTime(), batch.AtTime()
		if t1 < t2 {
			populate(bs.curr(), lt)
			bs.next()
		} else if t1 > t2 {
			populate(batch, rt)
			batch.Next()
		} else {
			if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
				// Prefer histograms than floats. Take left side if both have histograms.
				populate(batch, rt)
			} else {
				populate(bs.curr(), lt)
				// if bs.hPool is not nil, we put there the discarded histogram.Histogram object from batch, so it can be reused.
				if rt == chunkenc.ValHistogram && bs.hPool != nil {
					_, h := batch.AtHistogram()
					bs.hPool.Put((*histogram.Histogram)(h))
				}
				// if bs.fhPool is not nil, we put there the discarded histogram.FloatHistogram object from batch, so it can be reused.
				if rt == chunkenc.ValFloatHistogram && bs.fhPool != nil {
					_, fh := batch.AtFloatHistogram()
					bs.fhPool.Put((*histogram.FloatHistogram)(fh))
				}
			}
			bs.next()
			batch.Next()
		}
	}

	for t := bs.hasNext(); t != chunkenc.ValNone; t = bs.hasNext() {
		populate(bs.curr(), t)
		bs.next()
	}

	for t := batch.HasNext(); t != chunkenc.ValNone; t = batch.HasNext() {
		populate(batch, t)
		batch.Next()
	}

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	return bs.batchesBuf[:resultLen]
}

// badMerge is a concatenation of mergeStreams and merge, and shows a memory allocation degradation.
// It is literally a copy of mergeStreams and a copy of merge.
func (bs *mergeableBatchStream) badMerge(batch *chunk.Batch, size int) {
	origBsBatches := bs.batches[:0]
	// Reset the Index and Length of existing batches.
	for i := range bs.batchesBuf {
		bs.batchesBuf[i].Index = 0
		bs.batchesBuf[i].Length = 0
	}

	resultLen := 1 // Number of batches in the final result.
	b := &bs.batchesBuf[0]

	// Step to the next Batch in the result, create it if it does not exist
	nextBatch := func(valueType chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(bs.batchesBuf) {
			// It is possible that result can grow longer
			// then the one provided.
			bs.batchesBuf = append(bs.batchesBuf, chunk.Batch{})
		}
		b = &bs.batchesBuf[resultLen-1]
		b.ValueType = valueType
	}

	populate := func(batch *chunk.Batch, valueType chunkenc.ValueType) {
		if b.Index == 0 {
			// Starting to write this Batch, it is safe to set the value type
			b.ValueType = valueType
		} else if b.Index == size || b.ValueType != valueType {
			// The batch reached its intended size or is of a different value type
			// Add another batch to the result and use it for further appending.
			nextBatch(valueType)
		}

		switch valueType {
		case chunkenc.ValFloat:
			b.Timestamps[b.Index], b.Values[b.Index] = batch.At()
		case chunkenc.ValHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = batch.AtHistogram()
		case chunkenc.ValFloatHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = batch.AtFloatHistogram()
		}
		b.Index++
	}

	for lt, rt := bs.hasNext(), batch.HasNext(); lt != chunkenc.ValNone && rt != chunkenc.ValNone; lt, rt = bs.hasNext(), batch.HasNext() {
		t1, t2 := bs.curr().AtTime(), batch.AtTime()
		if t1 < t2 {
			populate(bs.curr(), lt)
			bs.next()
		} else if t1 > t2 {
			populate(batch, rt)
			batch.Next()
		} else {
			if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
				// Prefer histograms than floats. Take left side if both have histograms.
				populate(batch, rt)
			} else {
				populate(bs.curr(), lt)
				// if bs.hPool is not nil, we put there the discarded histogram.Histogram object from batch, so it can be reused.
				if rt == chunkenc.ValHistogram && bs.hPool != nil {
					_, h := batch.AtHistogram()
					bs.hPool.Put((*histogram.Histogram)(h))
				}
				// if bs.fhPool is not nil, we put there the discarded histogram.FloatHistogram object from batch, so it can be reused.
				if rt == chunkenc.ValFloatHistogram && bs.fhPool != nil {
					_, fh := batch.AtFloatHistogram()
					bs.fhPool.Put((*histogram.FloatHistogram)(fh))
				}
			}
			bs.next()
			batch.Next()
		}
	}

	for t := bs.hasNext(); t != chunkenc.ValNone; t = bs.hasNext() {
		populate(bs.curr(), t)
		bs.next()
	}

	for t := batch.HasNext(); t != chunkenc.ValNone; t = batch.HasNext() {
		populate(batch, t)
		batch.Next()
	}

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	bs.batches = append(origBsBatches, bs.batchesBuf[:resultLen]...)
	bs.reset()
}
