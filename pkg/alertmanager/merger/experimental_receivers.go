// SPDX-License-Identifier: AGPL-3.0-only

package merger

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/go-openapi/swag"
	am_models "github.com/grafana/mimir/pkg/alertmanager/models"
)

// V2Alerts implements the Merger interface for GET /v2/alerts. It returns the union
// of alerts over all the responses. When the same alert exists in multiple responses, the
// instance of that alert with the most recent UpdatedAt timestamp is returned in the response.
type ExperimentalReceivers struct{}

func (ExperimentalReceivers) MergeResponses(in [][]byte) ([]byte, error) {
	responses := make([]am_models.Receiver, 0)
	for _, body := range in {
		parsed := make([]am_models.Receiver, 0)
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, err
		}
		responses = append(responses, parsed...)
	}

	merged, err := mergeReceivers(responses)
	if err != nil {
		return nil, err
	}

	return json.Marshal(merged)
}

func mergeReceivers(in []am_models.Receiver) ([]am_models.Receiver, error) {
	receivers := make(map[string]am_models.Receiver)
	for _, recv := range in {
		if recv.Name == nil {
			return nil, errors.New("unexpected nil Name")
		}
		name := *recv.Name

		if current, ok := receivers[name]; ok {
			receivers[name] = mergeReceiver(receivers[name], recv)
		} else {
			receivers[name] = recv
		}
	}

	result := make([]am_models.Receiver, 0, len(receivers))
	for _, recv := range receivers {
		result = append(result, recv)
	}

	// Sort receivers by name to give a stable response.
	sort.Slice(result, func(i, j int) bool {
		return *result[i].Name < *result[j].Name
	})

	return result, nil
}

func mergeReceiver(lhs am_models.Receiver, rhs am_models.Receiver) am_models.Receiver {
	// Receiver is active if it's active anywhere. In theory these should never
	// be different, but perhaps during reconfiguration, one replica thinks
	// a receiver is active, and another doesn't.
	active := *lhs.Active || *rhs.Active

	// if not the same integrations, pick the smallest
	// we don't know which is the right config, so return something

	// same, merge each
	// if name differs, pick the smallest, config drift?

	// same amount, same name, pick the most recent notify attempt
}
