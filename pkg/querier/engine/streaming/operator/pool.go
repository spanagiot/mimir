// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"sync"

	"github.com/grafana/mimir/pkg/util/pool"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// TODO: are there generic versions of pool.Pool and sync.Pool that we can use, and then eliminate the helper functions?
// TODO: do we need to nil-out slice elements as well, to avoid holding on to elements for too long?
var (
	// TODO: what is a reasonable upper limit here?
	fPointSlicePool = pool.NewBucketedPool(1, 100000, 10, func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size)
	})

	// TODO: what is a reasonable upper limit here?
	matrixPool = pool.NewBucketedPool(1, 100000, 10, func(size int) promql.Matrix {
		return make(promql.Matrix, 0, size)
	})

	// TODO: what is a reasonable upper limit here?
	vectorPool = pool.NewBucketedPool(1, 100000, 10, func(size int) promql.Vector {
		return make(promql.Vector, 0, size)
	})

	// TODO: what is a reasonable upper limit here?
	seriesMetadataSlicePool = pool.NewBucketedPool(1, 100000, 10, func(size int) []SeriesMetadata {
		return make([]SeriesMetadata, 0, size)
	})

	seriesBatchPool = sync.Pool{New: func() any {
		return &SeriesBatch{
			series: make([]storage.Series, 0, 100), // TODO: what is a reasonable batch size?
			next:   nil,
		}
	}}

	// TODO: what is a reasonable upper limit here?
	floatSlicePool = pool.NewBucketedPool(1, 100000, 10, func(_ int) []float64 {
		// Don't allocate a new slice now - we'll allocate one in GetFloatSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})

	// TODO: what is a reasonable upper limit here?
	boolSlicePool = pool.NewBucketedPool(1, 100000, 10, func(_ int) []bool {
		// Don't allocate a new slice now - we'll allocate one in GetBoolSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})
)

func GetFPointSlice(size int) []promql.FPoint {
	return fPointSlicePool.Get(size)
}

func PutFPointSlice(s []promql.FPoint) {
	if s != nil {
		fPointSlicePool.Put(s)
	}
}

func GetMatrix(size int) promql.Matrix {
	return matrixPool.Get(size)
}

func PutMatrix(m promql.Matrix) {
	if m != nil {
		matrixPool.Put(m)
	}
}

func GetVector(size int) promql.Vector {
	return vectorPool.Get(size)
}

func PutVector(v promql.Vector) {
	if v != nil {
		vectorPool.Put(v)
	}
}

func GetSeriesMetadataSlice(size int) []SeriesMetadata {
	return seriesMetadataSlicePool.Get(size)
}

func PutSeriesMetadataSlice(s []SeriesMetadata) {
	if s != nil {
		seriesMetadataSlicePool.Put(s)
	}
}

func GetSeriesBatch() *SeriesBatch {
	return seriesBatchPool.Get().(*SeriesBatch)
}

func PutSeriesBatch(b *SeriesBatch) {
	if b != nil {
		b.series = b.series[:0]
		b.next = nil
		seriesBatchPool.Put(b)
	}
}

func GetFloatSlice(size int) []float64 {
	s := floatSlicePool.Get(size)
	if s != nil {
		return zeroFloatSlice(s, size)
	}

	return make([]float64, 0, size)
}

func PutFloatSlice(s []float64) {
	if s != nil {
		floatSlicePool.Put(s)
	}
}

func GetBoolSlice(size int) []bool {
	s := boolSlicePool.Get(size)

	if s != nil {
		return zeroBoolSlice(s, size)
	}

	return make([]bool, 0, size)
}

func PutBoolSlice(s []bool) {
	if s != nil {
		boolSlicePool.Put(s)
	}
}

func zeroFloatSlice(s []float64, size int) []float64 {
	s = s[:size]

	for i := range s {
		s[i] = 0
	}

	return s[:0]
}

func zeroBoolSlice(s []bool, size int) []bool {
	s = s[:size]

	for i := range s {
		s[i] = false
	}

	return s[:0]
}
