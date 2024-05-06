// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodingWithExemplarDecodingWithoutExemplar(t *testing.T) {
	reqWithExemplar := TimeSeries{
		Labels: []LabelAdapter{{
			Name:  "foo",
			Value: "bar",
		}},
		Samples: []Sample{{
			TimestampMs: 123,
			Value:       321,
		}},
		Exemplars: []Exemplar{{
			Labels: []LabelAdapter{{
				Name:  "foo",
				Value: "bar",
			}},
			TimestampMs: 123,
			Value:       321,
		}},
		Histograms: []Histogram{{
			Sum:           1,
			Schema:        2,
			ZeroThreshold: 3,
			ResetHint:     4,
			Timestamp:     5,
		}},
	}

	encoded, err := reqWithExemplar.Marshal()
	require.NoError(t, err)

	var reqWithoutExemplar TimeSeriesNoExemplars
	err = reqWithoutExemplar.Unmarshal(encoded)
	require.NoError(t, err)

	require.Equal(t, reqWithExemplar.Labels, reqWithoutExemplar.Labels)
	require.Equal(t, reqWithExemplar.Samples, reqWithoutExemplar.Samples)
	require.Equal(t, reqWithExemplar.Histograms, reqWithoutExemplar.Histograms)
}
