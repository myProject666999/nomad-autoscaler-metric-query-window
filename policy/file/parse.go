// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package file

import (
	"fmt"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsimple"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

var filePolicyEvalContext = &hcl.EvalContext{
	Functions: map[string]function.Function{
		"jsonencode": stdlib.JSONEncodeFunc,
	},
}

func decodeFile(file string) (map[string]*sdk.ScalingPolicy, error) {
	policies := make(map[string]*sdk.ScalingPolicy)

	filePolicies := sdk.FileDecodeScalingPolicies{}
	if err := hclsimple.DecodeFile(file, filePolicyEvalContext, &filePolicies); err != nil {
		return nil, err
	}

	var mErr *multierror.Error
	for _, p := range filePolicies.ScalingPolicies {
		if err := decodePolicyDoc(p); err != nil {
			mErr = multierror.Append(mErr, multierror.Prefix(err, p.Name))
		}
		policies[p.Name] = p.Translate()
	}
	if mErr != nil {
		return nil, mErr.ErrorOrNil()
	}

	return policies, nil

}

func decodePolicyDoc(decodePolicy *sdk.FileDecodeScalingPolicy) error {
	// Assume file policies are cluster policies unless specificied.
	// TODO: revisit this assumption.
	if decodePolicy.Type == "" {
		decodePolicy.Type = sdk.ScalingPolicyTypeCluster
	}

	if decodePolicy.Doc.CooldownHCL != "" {
		d, err := time.ParseDuration(decodePolicy.Doc.CooldownHCL)
		if err != nil {
			return err
		}
		decodePolicy.Doc.Cooldown = d
	}

	if decodePolicy.Doc.CooldownOnScaleUpHCL != "" {
		d, err := time.ParseDuration(decodePolicy.Doc.CooldownOnScaleUpHCL)
		if err != nil {
			return err
		}
		decodePolicy.Doc.CooldownOnScaleUp = d
	}

	if decodePolicy.Doc.EvaluationIntervalHCL != "" {
		d, err := time.ParseDuration(decodePolicy.Doc.EvaluationIntervalHCL)
		if err != nil {
			return err
		}
		decodePolicy.Doc.EvaluationInterval = d
	}

	// Parse query window for each check.
	for i := 0; i < len(decodePolicy.Doc.Checks); i++ {
		check := decodePolicy.Doc.Checks[i]

		if check.QueryWindowHCL != "" {
			if check.QueryWindowHCL != "instant" {
				w, err := parseNonNegativeDuration(check.QueryWindowHCL, "query_window", check.Name)
				if err != nil {
					return err
				}
				decodePolicy.Doc.Checks[i].QueryWindow = w
			}
		}

		if check.QueryWindowOffsetHCL != "" {
			o, err := parseNonNegativeDuration(check.QueryWindowOffsetHCL, "query_window_offset", check.Name)
			if err != nil {
				return err
			}
			decodePolicy.Doc.Checks[i].QueryWindowOffset = o
		}

		if check.QueryMaxStalenessHCL != "" {
			s, err := parseNonNegativeDuration(check.QueryMaxStalenessHCL, "query_max_staleness", check.Name)
			if err != nil {
				return err
			}
			decodePolicy.Doc.Checks[i].QueryMaxStaleness = s
		}

		if check.QueryMinPoints < 0 {
			return fmt.Errorf("check %s: query_min_points must not be negative", check.Name)
		}
	}

	return nil
}

// parseNonNegativeDuration parses a time.Duration and rejects negative
// values, which have no valid meaning for check query parameters.
func parseNonNegativeDuration(v, key, checkName string) (time.Duration, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("check %s: %s must not be negative", checkName, key)
	}
	return d, nil
}
