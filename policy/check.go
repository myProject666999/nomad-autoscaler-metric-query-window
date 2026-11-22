// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package policy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	metrics "github.com/hashicorp/go-metrics"
	"github.com/hashicorp/nomad-autoscaler/plugins"
	"github.com/hashicorp/nomad-autoscaler/plugins/apm"
	"github.com/hashicorp/nomad-autoscaler/plugins/strategy"
	"github.com/hashicorp/nomad-autoscaler/sdk"
)

var nowFunc = time.Now

var (
	// errCheckOutsideSchedule signals that a check fell outside its configured
	// schedule window and should be skipped from winner selection. The caller in
	// the handler treats it as a non-fatal skip; any other error aborts the
	// evaluation.
	errCheckOutsideSchedule = errors.New("check is not within defined schedule")

	// errCheckSkipped signals that a check failed to retrieve metrics but its
	// error handling configuration (check on_error or policy on_check_error)
	// downgraded the failure to a non-fatal skip. The caller in the handler
	// skips the check and continues evaluating the remaining checks.
	errCheckSkipped = errors.New("check skipped due to on_error configuration")

	// errMetricsQueryFailed classifies errors returned by the APM plugin when
	// executing the check query.
	errMetricsQueryFailed = errors.New("failed to query metrics source")

	// errNoMetrics classifies queries that returned no usable data points,
	// either because the APM returned an empty result or because every
	// returned point was dropped by the safety filters.
	errNoMetrics = errors.New("no metrics available")

	// errInsufficientMetrics classifies results that hold valid data but do
	// not satisfy the check's optional sufficiency rules (query_min_points,
	// query_max_staleness).
	errInsufficientMetrics = errors.New("insufficient metrics data")
)

type CheckRunnerConfig struct {
	// Log is the logger to use for logging.
	Log hclog.Logger

	// StrategyRunner is the strategy runner for the check.
	StrategyRunner strategy.Runner

	// MetricsGetter is the metrics getter for the check.
	MetricsGetter apm.Looker

	Policy *sdk.ScalingPolicy
}

// checkHandler evaluates one of the checks of a policy.
type checkRunner struct {
	log            hclog.Logger
	strategyRunner strategy.Runner
	metricsGetter  apm.Looker
	check          *sdk.ScalingPolicyCheck
	policy         *sdk.ScalingPolicy
	schedule       *compiledSchedule
	scheduleErr    error
	scheduleOnce   sync.Once
}

type queryMetricsCacheKey struct {
	source            string
	query             string
	queryWindow       time.Duration
	queryWindowOffset time.Duration
	queryInstant      bool
}

type queryMetricsCacheEntry struct {
	metrics sdk.TimestampedMetrics
	err     error
}

type queryMetricsCache struct {
	entries map[queryMetricsCacheKey]queryMetricsCacheEntry
}

func newQueryMetricsCache() *queryMetricsCache {
	return &queryMetricsCache{entries: make(map[queryMetricsCacheKey]queryMetricsCacheEntry)}
}

func cloneMetrics(ms sdk.TimestampedMetrics) sdk.TimestampedMetrics {
	if len(ms) == 0 {
		return nil
	}

	cloned := make(sdk.TimestampedMetrics, len(ms))
	copy(cloned, ms)
	return cloned
}

// NewCheckRunner returns a new checkRunner instance.
func newCheckRunner(config *CheckRunnerConfig, c *sdk.ScalingPolicyCheck) *checkRunner {
	// An offset greater than or equal to the window is a legitimate way to
	// query historical data, but it is unusual enough to deserve a hint.
	// Instant queries ignore the window, so the hint does not apply to them.
	if !c.QueryInstant && c.QueryWindow > 0 && c.QueryWindowOffset >= c.QueryWindow {
		config.Log.Info("check query_window_offset is greater than or equal to query_window, "+
			"queries will read historical data",
			"check", c.Name, "query_window", c.QueryWindow,
			"query_window_offset", c.QueryWindowOffset)
	}

	return &checkRunner{
		log:            config.Log,
		check:          c,
		strategyRunner: config.StrategyRunner,
		metricsGetter:  config.MetricsGetter,
		policy:         config.Policy,
	}
}

func (ch *checkRunner) ensureCompiledSchedule() error {
	ch.scheduleOnce.Do(func() {
		ch.schedule, ch.scheduleErr = compileSchedule(ch.check.Schedule)
	})
	return ch.scheduleErr
}

// ignoreCheckErrors reports whether a check failure should be downgraded to a
// non-fatal outcome based on the check-level on_error and the policy-level
// on_check_error. Recognized check-level values take precedence; unset or
// unrecognized values fall back to the policy value, which is non-fatal
// unless set to "fail".
func (ch *checkRunner) ignoreCheckErrors() bool {
	switch ch.check.OnError {
	case sdk.ScalingPolicyOnErrorIgnore:
		return true
	case sdk.ScalingPolicyOnErrorFail:
		return false
	default:
		return ch.policy.OnCheckError != sdk.ScalingPolicyOnErrorFail
	}
}

// getNewCountFromStrategy runs the strategy for one check and returns a
// scaling action, applying check-level and policy-level error handling.
func (ch *checkRunner) getNewCountFromStrategy(ctx context.Context, currentCount int64,
	metrics sdk.TimestampedMetrics) (sdk.ScalingAction, error) {
	ch.log.Debug("calculating new count", "current count", currentCount)

	a, err := ch.runStrategy(ctx, currentCount, metrics)
	if err != nil {
		ch.log.Warn("failed to run check", "check", ch.check.Name,
			"on_error", ch.check.OnError, "on_check_error",
			ch.policy.OnCheckError, "error", err)

		// Non-fatal path: return an empty action and continue evaluating the
		// remaining checks in the policy.
		if ch.ignoreCheckErrors() {
			return sdk.ScalingAction{}, nil
		}

		return sdk.ScalingAction{}, err
	}

	return a, nil
}

func (ch *checkRunner) runStrategy(ctx context.Context, currentCount int64, ms sdk.TimestampedMetrics) (sdk.ScalingAction, error) {
	checkEval := &sdk.ScalingCheckEvaluation{
		Check: ch.check,
		Action: &sdk.ScalingAction{
			Meta: map[string]interface{}{
				"nomad_policy_id": ch.policy.ID,
			},
		},
		Metrics: ms,
	}

	// Trigger a metric measure to track latency of the call.
	labels := []metrics.Label{
		{Name: "plugin_name", Value: ch.check.Strategy.Name},
		{Name: "policy_id", Value: ch.policy.ID},
	}

	defer metrics.MeasureSinceWithLabels([]string{"plugin", "strategy", "run", "invoke_ms"}, time.Now(), labels)

	var runResp *sdk.ScalingCheckEvaluation
	var err error

	ch.log.Debug("running strategy", "strategy", ch.check.Strategy.Name, "check", ch.check.Name)

	// Wrap call in a goroutine so we can listen for ctx as well.
	strategyRunDoneCh := make(chan interface{})
	go func() {
		defer close(strategyRunDoneCh)
		runResp, err = ch.strategyRunner.Run(checkEval, currentCount)
	}()

	select {
	case <-ctx.Done():
		return sdk.ScalingAction{}, ctx.Err()
	case <-strategyRunDoneCh:
	}

	if err != nil {
		return sdk.ScalingAction{}, fmt.Errorf("failed to execute strategy: %w", err)
	}

	if runResp == nil {
		return sdk.ScalingAction{}, nil
	}

	return *runResp.Action, nil
}

// queryMetrics wraps the apm.Query call to provide operational functionality.
//
// The query window is computed as [now-offset-window, now-offset]; for
// instant queries from == to == now-offset. Returned metrics are filtered by
// filterMetrics and then checked against the optional per-check sufficiency
// rules (query_min_points, query_max_staleness).
//
// Within a single evaluation round, results are shared through the
// queryMetricsCache between checks with the same source, query, window,
// offset and instant flag; errors are cached as well. The sufficiency rules
// are intentionally evaluated outside the cache because checks sharing a
// cached result may configure different limits.
func (ch *checkRunner) queryMetrics(ctx context.Context, cache *queryMetricsCache) (sdk.TimestampedMetrics, error) {
	ch.log.Debug("querying source", "query", ch.check.Query, "source", ch.check.Source)

	// Calculate query range from the query window defined in the ch.check.
	to := nowFunc().Add(-ch.check.QueryWindowOffset)
	from := to.Add(-ch.check.QueryWindow)
	if ch.check.QueryInstant {
		from = to
	}
	r := sdk.TimeRange{From: from, To: to}

	ms, err := ch.fetchMetrics(ctx, cache, r)
	if err != nil {
		return sdk.TimestampedMetrics{}, err
	}

	if err := ch.validateMetrics(ms, to); err != nil {
		return sdk.TimestampedMetrics{}, err
	}

	return ms, nil
}

// fetchMetrics performs the APM query for the check, consulting and
// populating the shared cache, and applies the safety filters to the
// returned data points.
func (ch *checkRunner) fetchMetrics(ctx context.Context, cache *queryMetricsCache, r sdk.TimeRange) (sdk.TimestampedMetrics, error) {
	cacheKey := queryMetricsCacheKey{
		source:            ch.check.Source,
		query:             ch.check.Query,
		queryWindow:       ch.check.QueryWindow,
		queryWindowOffset: ch.check.QueryWindowOffset,
		queryInstant:      ch.check.QueryInstant,
	}

	if cache != nil {
		if entry, ok := cache.entries[cacheKey]; ok {
			ch.log.Trace("using cached query result")
			if entry.err != nil {
				return sdk.TimestampedMetrics{}, entry.err
			}
			return cloneMetrics(entry.metrics), nil
		}
	}

	// Trigger a metric measure to track latency of the call.
	labels := []metrics.Label{{Name: "plugin_name", Value: ch.check.Source},
		{Name: "policy_id", Value: ch.policy.ID}}
	defer metrics.MeasureSinceWithLabels([]string{"plugin", "apm", "query", "invoke_ms"},
		time.Now(), labels)

	var err error
	ms := sdk.TimestampedMetrics{}

	// Query check's APM.
	// Wrap call in a goroutine so we can listen for ctx as well.
	apmQueryDoneCh := make(chan interface{})
	go func() {
		defer close(apmQueryDoneCh)
		ms, err = ch.metricsGetter.Query(ch.check.Query, r)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-apmQueryDoneCh:
	}

	if err != nil {
		queryErr := fmt.Errorf("%w: %w", errMetricsQueryFailed, err)
		if cache != nil {
			cache.entries[cacheKey] = queryMetricsCacheEntry{err: queryErr}
		}
		return sdk.TimestampedMetrics{}, queryErr
	}

	// Drop points that are unsafe or outside the query window before
	// deciding whether the result is empty.
	ms = filterMetrics(ms, r, ch.check.QueryInstant)

	if len(ms) == 0 {
		if cache != nil {
			cache.entries[cacheKey] = queryMetricsCacheEntry{err: errNoMetrics}
		}
		return sdk.TimestampedMetrics{}, errNoMetrics
	}

	if cache != nil {
		cache.entries[cacheKey] = queryMetricsCacheEntry{metrics: cloneMetrics(ms)}
	}

	return ms, nil
}

// filterMetrics drops data points that are unsafe or out of scope and
// normalizes the result:
//
//   - points with NaN or +/-Inf values are always dropped;
//   - for range queries, points outside the closed interval [from, to] are
//     dropped. Instant queries skip window filtering entirely: the plugin
//     contract (From == To) allows the returned point's timestamp to differ
//     from `to`, so range filtering could empty otherwise valid results;
//   - remaining points are sorted by timestamp with a stable sort, so points
//     sharing a timestamp keep the order in which the plugin returned them;
//   - duplicate timestamps are deduplicated keeping the point that appeared
//     last in the plugin's response (last write wins).
func filterMetrics(ms sdk.TimestampedMetrics, r sdk.TimeRange, instant bool) sdk.TimestampedMetrics {
	filtered := make(sdk.TimestampedMetrics, 0, len(ms))
	for _, m := range ms {
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			continue
		}
		if !instant && (m.Timestamp.Before(r.From) || m.Timestamp.After(r.To)) {
			continue
		}
		filtered = append(filtered, m)
	}

	sort.Stable(filtered)

	deduped := filtered[:0]
	for i, m := range filtered {
		// If the next point has the same timestamp, skip this one so the
		// last occurrence in the response order is kept.
		if i+1 < len(filtered) && filtered[i+1].Timestamp.Equal(m.Timestamp) {
			continue
		}
		deduped = append(deduped, m)
	}

	return deduped
}

// validateMetrics applies the optional per-check data sufficiency rules to
// the filtered metrics. The to parameter is the end of the query window and
// is used as the reference for staleness.
func (ch *checkRunner) validateMetrics(ms sdk.TimestampedMetrics, to time.Time) error {
	// The minimum point count does not apply to instant queries, which are
	// expected to return a single point.
	if !ch.check.QueryInstant && ch.check.QueryMinPoints > 0 && len(ms) < ch.check.QueryMinPoints {
		return fmt.Errorf("%w: %d valid data points, minimum is %d",
			errInsufficientMetrics, len(ms), ch.check.QueryMinPoints)
	}

	if ch.check.QueryMaxStaleness > 0 && len(ms) > 0 {
		age := to.Sub(ms[len(ms)-1].Timestamp)
		if age > ch.check.QueryMaxStaleness {
			return fmt.Errorf("%w: newest data point is %s behind the query window end, maximum staleness is %s",
				errInsufficientMetrics, age.Round(time.Millisecond), ch.check.QueryMaxStaleness)
		}
	}

	return nil
}

// metricErrorType classifies a metric retrieval failure for log fields and
// metric labels so the three failure modes can be counted separately.
func metricErrorType(err error) string {
	switch {
	case errors.Is(err, errInsufficientMetrics):
		return "insufficient_data"
	case errors.Is(err, errNoMetrics):
		return "empty_result"
	default:
		return "query_error"
	}
}

func (ch *checkRunner) group() string {
	return ch.check.Group
}

// runCheckAndCapCount evaluates the check and caps the returned action.
//
// The method returns errCheckOutsideSchedule when the check is outside its
// schedule window, and errCheckSkipped when a metric retrieval failure was
// downgraded to a skip by the check on_error or policy on_check_error
// configuration; the caller should skip this check and continue in both
// cases. Any other error, including context errors, should abort the
// evaluation.
func (ch *checkRunner) runCheckAndCapCount(ctx context.Context, currentCount int64, cache *queryMetricsCache) (sdk.ScalingAction, error) {
	ch.log.Debug("received policy check for evaluation")

	if err := ch.ensureCompiledSchedule(); err != nil {
		return sdk.ScalingAction{}, fmt.Errorf("failed to compile check schedule: %w", err)
	}

	if ch.schedule != nil && !ch.schedule.activeAt(nowFunc()) {
		return sdk.ScalingAction{}, errCheckOutsideSchedule
	}

	ms := sdk.TimestampedMetrics{}
	// Fixed-value strategy does not require APM metrics; all other strategies still do.
	if ch.check.Strategy.Name != plugins.InternalStrategyFixedValue {
		var err error
		ms, err = ch.queryMetrics(ctx, cache)
		if err != nil {
			// Context cancellation and expiration always abort the
			// evaluation; on_error handling only applies to genuine query
			// and data failures.
			if ctx.Err() != nil {
				return sdk.ScalingAction{}, fmt.Errorf("failed to query metrics: %w", err)
			}

			ch.log.Warn("failed to query metrics for check", "check", ch.check.Name,
				"error_type", metricErrorType(err),
				"on_error", ch.check.OnError, "on_check_error",
				ch.policy.OnCheckError, "error", err)

			// Count metric failures by type so query errors, empty results
			// and insufficient data can be tracked separately.
			labels := []metrics.Label{
				{Name: "plugin_name", Value: ch.check.Source},
				{Name: "policy_id", Value: ch.policy.ID},
				{Name: "check", Value: ch.check.Name},
				{Name: "error_type", Value: metricErrorType(err)},
			}
			metrics.IncrCounterWithLabels([]string{"policy", "check", "metrics", "error_count"}, 1, labels)

			// Non-fatal path: skip this check entirely and continue
			// evaluating the remaining checks in the policy.
			if ch.ignoreCheckErrors() {
				return sdk.ScalingAction{}, errCheckSkipped
			}

			return sdk.ScalingAction{}, fmt.Errorf("failed to query metrics: %w", err)
		}
	}

	action, err := ch.getNewCountFromStrategy(ctx, currentCount, ms)
	if err != nil {
		return sdk.ScalingAction{}, fmt.Errorf("failed get count from metrics: %w", err)
	}

	action.CapCount(ch.policy.Min, ch.policy.Max)

	return action, nil
}
