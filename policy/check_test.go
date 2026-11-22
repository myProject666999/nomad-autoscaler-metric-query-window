// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package policy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins"
	"github.com/hashicorp/nomad-autoscaler/plugins/apm"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/shoenig/test"
	"github.com/shoenig/test/must"
)

var errTest = errors.New("error")

type mockStrategyRunner struct {
	t         *testing.T
	count     int64
	direction sdk.ScaleDirection
	runCalls  int

	err error
}

func (m *mockStrategyRunner) Run(eval *sdk.ScalingCheckEvaluation, count int64) (*sdk.ScalingCheckEvaluation, error) {
	m.runCalls++
	test.NotNil(m.t, eval)
	test.NotNil(m.t, eval.Metrics)
	eval.Action.Count = m.count
	eval.Action.Direction = m.direction

	return eval, m.err
}

type mockAPMLooker struct {
	t          *testing.T
	query      string
	timeRange  sdk.TimeRange
	metrics    sdk.TimestampedMetrics
	err        error
	queryCalls int
}

func (m *mockAPMLooker) Query(query string, timeRange sdk.TimeRange) (sdk.TimestampedMetrics, error) {
	m.queryCalls++
	must.StrContains(m.t, m.query, query)
	must.Eq(m.t, m.timeRange, timeRange)

	return m.metrics, m.err
}

func (m *mockAPMLooker) QueryMultiple(query string, timeRange sdk.TimeRange) ([]sdk.TimestampedMetrics, error) {
	return nil, nil
}

var testPolicy = &sdk.ScalingPolicy{
	ID:  "testPolicy",
	Min: 1,
	Max: 10,
}

var testPolicyOnErrorIgnore = &sdk.ScalingPolicy{
	ID:           "testPolicyOnErrorIgnore",
	Min:          1,
	Max:          10,
	OnCheckError: sdk.ScalingPolicyOnErrorIgnore,
}

var testPolicyOnErrorFail = &sdk.ScalingPolicy{
	ID:           "testPolicyOnErrorFail",
	Min:          1,
	Max:          10,
	OnCheckError: sdk.ScalingPolicyOnErrorFail,
}

func TestCheckHandler_getNewCountFromMetrics(t *testing.T) {
	tests := []struct {
		name           string
		metrics        sdk.TimestampedMetrics
		policy         *sdk.ScalingPolicy
		checkOnError   string
		runErr         error
		expError       error
		expectedAction sdk.ScalingAction
	}{
		{
			name:           "ignore_on_check_error",
			checkOnError:   sdk.ScalingPolicyOnErrorIgnore,
			runErr:         errTest,
			policy:         testPolicy,
			metrics:        sdk.TimestampedMetrics{},
			expError:       nil,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:           "ignore_on_check_error_from_check",
			checkOnError:   "",
			runErr:         errTest,
			policy:         testPolicyOnErrorIgnore,
			metrics:        sdk.TimestampedMetrics{},
			expError:       nil,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:           "default_on_check_error_from_policy",
			checkOnError:   "",
			runErr:         errTest,
			policy:         testPolicy,
			metrics:        sdk.TimestampedMetrics{},
			expError:       nil,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:           "unexpected_check_on_error_falls_back_to_policy_fail",
			checkOnError:   "unexpected",
			runErr:         errTest,
			policy:         testPolicyOnErrorFail,
			metrics:        sdk.TimestampedMetrics{},
			expError:       errTest,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:           "unexpected_check_on_error_falls_back_to_policy_ignore",
			checkOnError:   "unexpected",
			runErr:         errTest,
			policy:         testPolicy,
			metrics:        sdk.TimestampedMetrics{},
			expError:       nil,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:           "fail_on_check_error_from_policy",
			checkOnError:   "",
			runErr:         errTest,
			policy:         testPolicyOnErrorFail,
			metrics:        sdk.TimestampedMetrics{},
			expError:       errTest,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:           "fail_on_check_error_from_check",
			checkOnError:   sdk.ScalingPolicyOnErrorFail,
			runErr:         errTest,
			policy:         testPolicy,
			metrics:        sdk.TimestampedMetrics{},
			expError:       errTest,
			expectedAction: sdk.ScalingAction{},
		},
		{
			name:         "success",
			checkOnError: sdk.ScalingPolicyOnErrorFail,
			runErr:       nil,
			policy:       testPolicy,
			metrics:      sdk.TimestampedMetrics{},
			expError:     nil,
			expectedAction: sdk.ScalingAction{
				Meta: map[string]interface{}{
					"nomad_policy_id": testPolicy.ID,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sr := &mockStrategyRunner{
				t:   t,
				err: tt.runErr,
			}

			ch := &sdk.ScalingPolicyCheck{
				OnError: tt.checkOnError,
				Strategy: &sdk.ScalingPolicyStrategy{
					Name: "strategy",
				},
			}

			runner := newCheckRunner(&CheckRunnerConfig{
				Log:            hclog.NewNullLogger(),
				StrategyRunner: sr,
				Policy:         tt.policy,
			}, ch)

			action, err := runner.getNewCountFromStrategy(context.Background(), 3, tt.metrics)
			must.Eq(t, tt.expectedAction, action)
			must.Eq(t, tt.expError, errors.Unwrap(err))

		})
	}
}

func TestCheckHandler_runAPMQuery(t *testing.T) {
	nowFunc = func() time.Time {
		return time.Date(2020, 1, 1, 0, 10, 0, 0, time.UTC)
	}
	now := nowFunc()
	testQuery := "rate(http_requests_total[5m])"
	testWindow := 10 * time.Minute

	testMetrics := sdk.TimestampedMetrics{
		{Timestamp: now.Add(-time.Minute), Value: 1.0},
		{Timestamp: now, Value: 2.0},
	}

	mockLooker := &mockAPMLooker{
		t:     t,
		query: testQuery,
	}

	tests := []struct {
		name       string
		instant    bool
		metrics    sdk.TimestampedMetrics
		queryError error
		expResult  sdk.TimestampedMetrics
		expErr     error
	}{
		{
			name:      "valid_metrics_returned",
			metrics:   testMetrics,
			expErr:    nil,
			expResult: testMetrics,
		},
		{
			name:      "valid_metrics_returned_instant_query_window",
			instant:   true,
			metrics:   testMetrics,
			expErr:    nil,
			expResult: testMetrics,
		},
		{
			name:       "query_error",
			queryError: errTest,
			expResult:  sdk.TimestampedMetrics{},
			expErr:     errTest,
		},
		{
			name:      "empty metrics",
			expResult: sdk.TimestampedMetrics{},
			expErr:    errNoMetrics,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockLooker.metrics = tc.metrics
			mockLooker.err = tc.queryError
			mockLooker.timeRange = sdk.TimeRange{From: now.Add(-testWindow), To: now}
			if tc.instant {
				mockLooker.timeRange = sdk.TimeRange{From: now, To: now}
			}

			check := &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             testQuery,
				QueryWindow:       testWindow,
				QueryWindowOffset: 0,
				QueryInstant:      tc.instant,
				Strategy: &sdk.ScalingPolicyStrategy{
					Name: "strategy",
				},
			}

			handler := newCheckRunner(&CheckRunnerConfig{
				Log:           hclog.NewNullLogger(),
				MetricsGetter: mockLooker,
				Policy: &sdk.ScalingPolicy{
					ID: "testPolicy",
				},
			}, check)

			result, err := handler.queryMetrics(context.Background(), nil)
			must.Eq(t, tc.expResult, result)
			must.True(t, errors.Is(err, tc.expErr))
		})
	}
}

func TestCheckHandler_runAPMQuery_UsesSharedCacheForIdenticalChecks(t *testing.T) {
	nowFunc = func() time.Time {
		return time.Date(2020, 1, 1, 0, 10, 0, 0, time.UTC)
	}

	now := nowFunc()
	query := "rate(http_requests_total[5m])"
	window := 10 * time.Minute
	metrics := sdk.TimestampedMetrics{
		{Timestamp: now.Add(-time.Minute), Value: 1.0},
		{Timestamp: now, Value: 2.0},
	}

	mockLooker := &mockAPMLooker{
		t:       t,
		query:   query,
		metrics: metrics,
		timeRange: sdk.TimeRange{
			From: now.Add(-window),
			To:   now,
		},
	}

	policy := &sdk.ScalingPolicy{ID: "testPolicy"}
	cache := newQueryMetricsCache()

	runnerA := newCheckRunner(&CheckRunnerConfig{
		Log:           hclog.NewNullLogger(),
		MetricsGetter: mockLooker,
		Policy:        policy,
	}, &sdk.ScalingPolicyCheck{
		Name:              "check-a",
		Source:            "mock",
		Query:             query,
		QueryWindow:       window,
		QueryWindowOffset: 0,
		Strategy: &sdk.ScalingPolicyStrategy{
			Name: "strategy",
		},
	})

	runnerB := newCheckRunner(&CheckRunnerConfig{
		Log:           hclog.NewNullLogger(),
		MetricsGetter: mockLooker,
		Policy:        policy,
	}, &sdk.ScalingPolicyCheck{
		Name:              "check-b",
		Source:            "mock",
		Query:             query,
		QueryWindow:       window,
		QueryWindowOffset: 0,
		Strategy: &sdk.ScalingPolicyStrategy{
			Name: "strategy",
		},
	})

	resultA, errA := runnerA.queryMetrics(context.Background(), cache)
	must.NoError(t, errA)
	resultB, errB := runnerB.queryMetrics(context.Background(), cache)
	must.NoError(t, errB)

	must.Eq(t, metrics, resultA)
	must.Eq(t, metrics, resultB)
	must.Eq(t, 1, mockLooker.queryCalls)
}

func TestCheckHandler_runCheckAndCapCount_OutsideSchedule(t *testing.T) {
	nowFunc = func() time.Time {
		return time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	}

	sr := &mockStrategyRunner{t: t, count: 10, direction: sdk.ScaleDirectionUp}
	ml := &mockAPMLooker{t: t, query: "query", metrics: sdk.TimestampedMetrics{{Timestamp: nowFunc(), Value: 1}}}

	runner := newCheckRunner(&CheckRunnerConfig{
		Log:            hclog.NewNullLogger(),
		StrategyRunner: sr,
		MetricsGetter:  ml,
		Policy:         testPolicy,
	}, &sdk.ScalingPolicyCheck{
		Name:        "scheduled-check",
		Source:      "mock",
		Query:       "query",
		QueryWindow: time.Minute,
		Schedule: &sdk.ScalingPolicySchedule{
			Start:    "0 9 * * *",
			Duration: "30m",
		},
		Strategy: &sdk.ScalingPolicyStrategy{Name: "strategy"},
	})

	action, err := runner.runCheckAndCapCount(context.Background(), 5, newQueryMetricsCache())
	errMsg := must.Sprint("policy check should not run outside a schedule window")
	must.True(t, errors.Is(err, errCheckOutsideSchedule), errMsg)
	must.Eq(t, sdk.ScalingAction{}, action, errMsg)
	must.Eq(t, 0, ml.queryCalls, errMsg)
	must.Eq(t, 0, sr.runCalls, errMsg)
}

func TestCheckHandler_runCheckAndCapCount_IgnoredStrategyErrorsContinueEvaluation(t *testing.T) {
	nowFunc = func() time.Time {
		return time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	}

	sr := &mockStrategyRunner{t: t, err: errTest}
	ml := &mockAPMLooker{
		t:       t,
		query:   "query",
		metrics: sdk.TimestampedMetrics{{Timestamp: nowFunc(), Value: 1}},
		timeRange: sdk.TimeRange{
			From: nowFunc().Add(-time.Minute),
			To:   nowFunc(),
		},
	}

	runner := newCheckRunner(&CheckRunnerConfig{
		Log:            hclog.NewNullLogger(),
		StrategyRunner: sr,
		MetricsGetter:  ml,
		Policy:         testPolicy,
	}, &sdk.ScalingPolicyCheck{
		Name:        "error-check",
		Source:      "mock",
		Query:       "query",
		QueryWindow: time.Minute,
		Strategy:    &sdk.ScalingPolicyStrategy{Name: "strategy"},
	})

	action, err := runner.runCheckAndCapCount(context.Background(), 5, newQueryMetricsCache())
	must.NoError(t, err)
	must.Eq(t, sdk.ScaleDirectionNone, action.Direction)

	// Side-effect sanity check: count is capped into policy bounds when the
	// ignored strategy error returns an empty action.
	must.Eq(t, testPolicy.Min, action.Count)
	capped, ok := action.Meta["nomad_autoscaler.count.capped"].(bool)
	must.True(t, ok)
	must.True(t, capped)
	original, ok := action.Meta["nomad_autoscaler.count.original"].(int64)
	must.True(t, ok)
	must.Eq(t, int64(0), original)
	must.Eq(t, 1, ml.queryCalls)
	must.Eq(t, 1, sr.runCalls)
}

func TestCheckHandler_runCheckAndCapCount_FixedValueSkipsAPMQuery(t *testing.T) {
	sr := &mockStrategyRunner{t: t, count: 7, direction: sdk.ScaleDirectionUp}
	ml := &mockAPMLooker{t: t, query: "query", err: errTest}

	runner := newCheckRunner(&CheckRunnerConfig{
		Log:            hclog.NewNullLogger(),
		StrategyRunner: sr,
		MetricsGetter:  ml,
		Policy:         testPolicy,
	}, &sdk.ScalingPolicyCheck{
		Name: "fixed-value-check",
		// source and query are not needed for fixed-value, because the APM query should be skipped entirely.
		QueryWindow: time.Minute,
		Strategy: &sdk.ScalingPolicyStrategy{
			Name: plugins.InternalStrategyFixedValue,
		},
	})

	action, err := runner.runCheckAndCapCount(context.Background(), 5, newQueryMetricsCache())
	must.NoError(t, err)
	test.Eq(t, int64(7), action.Count)
	test.Eq(t, sdk.ScaleDirectionUp, action.Direction)
	test.Eq(t, 0, ml.queryCalls, test.Sprint("should not have made any apm queries"))
	test.Eq(t, 1, sr.runCalls)
}

// fakeAPMLooker is a fake apm.Looker that records the time ranges it was
// queried with and returns programmed results in call order. When called
// more times than there are results, the last result is repeated.
type fakeAPMLooker struct {
	results []fakeAPMResult
	calls   int
	queries []string
	ranges  []sdk.TimeRange
}

type fakeAPMResult struct {
	metrics sdk.TimestampedMetrics
	err     error
}

func (m *fakeAPMLooker) Query(query string, r sdk.TimeRange) (sdk.TimestampedMetrics, error) {
	m.queries = append(m.queries, query)
	m.ranges = append(m.ranges, r)
	res := m.results[len(m.results)-1]
	if m.calls < len(m.results) {
		res = m.results[m.calls]
	}
	m.calls++
	return res.metrics, res.err
}

func (m *fakeAPMLooker) QueryMultiple(string, sdk.TimeRange) ([]sdk.TimestampedMetrics, error) {
	return nil, nil
}

// testNow is the fixed evaluation time used by the queryMetrics tests:
// 10:00:00 exactly.
var testNow = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

func fixNow(t *testing.T) {
	t.Helper()
	nowFunc = func() time.Time { return testNow }
	t.Cleanup(func() { nowFunc = time.Now })
}

// ts is a shorthand for building timestamps relative to testNow.
func ts(m, s int) time.Time {
	return testNow.Add(time.Duration(m)*time.Minute + time.Duration(s)*time.Second)
}

func newTestCheckRunner(looker apm.Looker, check *sdk.ScalingPolicyCheck, p *sdk.ScalingPolicy) *checkRunner {
	if p == nil {
		p = &sdk.ScalingPolicy{ID: "testPolicy"}
	}
	if check.Strategy == nil {
		check.Strategy = &sdk.ScalingPolicyStrategy{Name: "strategy"}
	}
	return newCheckRunner(&CheckRunnerConfig{
		Log:           hclog.NewNullLogger(),
		MetricsGetter: looker,
		Policy:        p,
	}, check)
}

func TestCheckHandler_queryMetrics_TimeRangeComputation(t *testing.T) {
	fixNow(t)

	testCases := []struct {
		name     string
		window   time.Duration
		offset   time.Duration
		instant  bool
		wantFrom time.Time
		wantTo   time.Time
	}{
		{
			name:     "window_and_offset",
			window:   5 * time.Minute,
			offset:   time.Minute,
			wantFrom: ts(-6, 0), // 09:54:00
			wantTo:   ts(-1, 0), // 09:59:00
		},
		{
			name:     "window_without_offset",
			window:   5 * time.Minute,
			offset:   0,
			wantFrom: ts(-5, 0), // 09:55:00
			wantTo:   ts(0, 0),  // 10:00:00
		},
		{
			name:     "instant_with_offset",
			window:   5 * time.Minute,
			offset:   time.Minute,
			instant:  true,
			wantFrom: ts(-1, 0), // 09:59:00
			wantTo:   ts(-1, 0), // 09:59:00
		},
		{
			name:     "instant_without_offset",
			window:   5 * time.Minute,
			offset:   0,
			instant:  true,
			wantFrom: ts(0, 0),
			wantTo:   ts(0, 0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looker := &fakeAPMLooker{results: []fakeAPMResult{{
				metrics: sdk.TimestampedMetrics{{Timestamp: tc.wantTo, Value: 1}},
			}}}

			runner := newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             "query",
				QueryWindow:       tc.window,
				QueryWindowOffset: tc.offset,
				QueryInstant:      tc.instant,
			}, nil)

			_, err := runner.queryMetrics(context.Background(), nil)
			must.NoError(t, err)
			must.Eq(t, 1, looker.calls)
			must.Eq(t, sdk.TimeRange{From: tc.wantFrom, To: tc.wantTo}, looker.ranges[0])
		})
	}
}

func TestCheckHandler_queryMetrics_Filtering(t *testing.T) {
	fixNow(t)

	// Window [09:54:00, 09:59:00] as specified: now=10:00:00, window=5m,
	// offset=1m.
	window := 5 * time.Minute
	offset := time.Minute

	testCases := []struct {
		name    string
		instant bool
		input   sdk.TimestampedMetrics
		want    sdk.TimestampedMetrics
	}{
		{
			name: "out_of_window_non_finite_and_duplicates_dropped",
			input: sdk.TimestampedMetrics{
				{Timestamp: ts(-6, -30), Value: 1},        // 09:53:30, before window
				{Timestamp: ts(-5, 0), Value: 2},          // 09:55:00, first occurrence
				{Timestamp: ts(-5, 0), Value: 3},          // 09:55:00, last occurrence wins
				{Timestamp: ts(-3, 0), Value: math.NaN()}, // 09:57:00, NaN
				{Timestamp: ts(0, -90), Value: 5},         // 09:58:30
			},
			want: sdk.TimestampedMetrics{
				{Timestamp: ts(-5, 0), Value: 3},
				{Timestamp: ts(0, -90), Value: 5},
			},
		},
		{
			name: "infinities_dropped",
			input: sdk.TimestampedMetrics{
				{Timestamp: ts(-4, 0), Value: math.Inf(1)},
				{Timestamp: ts(-3, 0), Value: math.Inf(-1)},
				{Timestamp: ts(-2, 0), Value: 7},
			},
			want: sdk.TimestampedMetrics{
				{Timestamp: ts(-2, 0), Value: 7},
			},
		},
		{
			name: "window_boundaries_are_closed",
			input: sdk.TimestampedMetrics{
				{Timestamp: ts(-6, 0), Value: 1},  // exactly from: kept
				{Timestamp: ts(-1, 0), Value: 2},  // exactly to: kept
				{Timestamp: ts(0, 0), Value: 3},   // after to: dropped
				{Timestamp: ts(-6, -1), Value: 4}, // before from: dropped
			},
			want: sdk.TimestampedMetrics{
				{Timestamp: ts(-6, 0), Value: 1},
				{Timestamp: ts(-1, 0), Value: 2},
			},
		},
		{
			name: "unsorted_input_is_sorted",
			input: sdk.TimestampedMetrics{
				{Timestamp: ts(-2, 0), Value: 3},
				{Timestamp: ts(-4, 0), Value: 1},
				{Timestamp: ts(-3, 0), Value: 2},
			},
			want: sdk.TimestampedMetrics{
				{Timestamp: ts(-4, 0), Value: 1},
				{Timestamp: ts(-3, 0), Value: 2},
				{Timestamp: ts(-2, 0), Value: 3},
			},
		},
		{
			name:    "instant_skips_window_filtering_but_not_non_finite",
			instant: true,
			input: sdk.TimestampedMetrics{
				// Timestamp far from `to` is still valid for instant queries.
				{Timestamp: ts(-6, -30), Value: 1},
				{Timestamp: ts(-1, 0), Value: math.NaN()},
			},
			want: sdk.TimestampedMetrics{
				{Timestamp: ts(-6, -30), Value: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looker := &fakeAPMLooker{results: []fakeAPMResult{{metrics: tc.input}}}

			runner := newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             "query",
				QueryWindow:       window,
				QueryWindowOffset: offset,
				QueryInstant:      tc.instant,
			}, nil)

			got, err := runner.queryMetrics(context.Background(), nil)
			must.NoError(t, err)
			must.Eq(t, tc.want, got)
		})
	}
}

func TestCheckHandler_queryMetrics_EmptyResults(t *testing.T) {
	fixNow(t)

	testCases := []struct {
		name  string
		input sdk.TimestampedMetrics
	}{
		{
			name:  "apm_returns_empty",
			input: sdk.TimestampedMetrics{},
		},
		{
			name: "all_points_filtered",
			input: sdk.TimestampedMetrics{
				{Timestamp: ts(-30, 0), Value: 1},          // outside window
				{Timestamp: ts(-2, 0), Value: math.NaN()},  // non-finite
				{Timestamp: ts(-3, 0), Value: math.Inf(1)}, // non-finite
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looker := &fakeAPMLooker{results: []fakeAPMResult{{metrics: tc.input}}}

			runner := newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             "query",
				QueryWindow:       5 * time.Minute,
				QueryWindowOffset: time.Minute,
			}, nil)

			_, err := runner.queryMetrics(context.Background(), nil)
			must.ErrorIs(t, err, errNoMetrics)
		})
	}
}

func TestCheckHandler_queryMetrics_MinPoints(t *testing.T) {
	fixNow(t)

	singlePoint := sdk.TimestampedMetrics{{Timestamp: ts(-2, 0), Value: 1}}

	testCases := []struct {
		name      string
		instant   bool
		minPoints int
		wantErr   error
	}{
		{name: "single_point_allowed_by_default", minPoints: 0},
		{name: "single_point_meets_minimum_of_one", minPoints: 1},
		{name: "single_point_below_minimum", minPoints: 2, wantErr: errInsufficientMetrics},
		{name: "instant_exempt_from_minimum", instant: true, minPoints: 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looker := &fakeAPMLooker{results: []fakeAPMResult{{metrics: singlePoint}}}

			runner := newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             "query",
				QueryWindow:       5 * time.Minute,
				QueryWindowOffset: time.Minute,
				QueryInstant:      tc.instant,
				QueryMinPoints:    tc.minPoints,
			}, nil)

			got, err := runner.queryMetrics(context.Background(), nil)
			if tc.wantErr != nil {
				must.ErrorIs(t, err, tc.wantErr)
				return
			}
			must.NoError(t, err)
			must.Eq(t, singlePoint, got)
		})
	}
}

func TestCheckHandler_queryMetrics_MaxStaleness(t *testing.T) {
	fixNow(t)

	// The newest point is 30s behind the query window end (to=09:59:00).
	metrics := sdk.TimestampedMetrics{
		{Timestamp: ts(-5, 0), Value: 1},
		{Timestamp: ts(-1, -30), Value: 2}, // 09:58:30
	}

	testCases := []struct {
		name         string
		instant      bool
		maxStaleness time.Duration
		wantErr      error
	}{
		{name: "unset_staleness_limit_allows_any_age", maxStaleness: 0},
		{name: "sixty_second_limit_passes", maxStaleness: 60 * time.Second},
		{name: "limit_equal_to_age_passes", maxStaleness: 30 * time.Second},
		{name: "twenty_second_limit_is_stale", maxStaleness: 20 * time.Second, wantErr: errInsufficientMetrics},
		{name: "instant_applies_staleness", instant: true, maxStaleness: 20 * time.Second, wantErr: errInsufficientMetrics},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looker := &fakeAPMLooker{results: []fakeAPMResult{{metrics: metrics}}}

			runner := newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             "query",
				QueryWindow:       5 * time.Minute,
				QueryWindowOffset: time.Minute,
				QueryInstant:      tc.instant,
				QueryMaxStaleness: tc.maxStaleness,
			}, nil)

			got, err := runner.queryMetrics(context.Background(), nil)
			if tc.wantErr != nil {
				must.ErrorIs(t, err, tc.wantErr)
				return
			}
			must.NoError(t, err)
			must.Eq(t, metrics, got)
		})
	}
}

func TestCheckHandler_queryMetricsCache_InstantNotShared(t *testing.T) {
	fixNow(t)

	window := 5 * time.Minute
	offset := time.Minute

	rangeMetrics := sdk.TimestampedMetrics{{Timestamp: ts(-2, 0), Value: 1}}
	instantMetrics := sdk.TimestampedMetrics{{Timestamp: ts(-1, 0), Value: 2}}

	looker := &fakeAPMLooker{results: []fakeAPMResult{
		{metrics: rangeMetrics},
		{metrics: instantMetrics},
	}}

	p := &sdk.ScalingPolicy{ID: "testPolicy"}
	cache := newQueryMetricsCache()

	mkRunner := func(instant bool) *checkRunner {
		return newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
			Name:              "check",
			Source:            "mock",
			Query:             "query",
			QueryWindow:       window,
			QueryWindowOffset: offset,
			QueryInstant:      instant,
		}, p)
	}

	gotRange, err := mkRunner(false).queryMetrics(context.Background(), cache)
	must.NoError(t, err)
	gotInstant, err := mkRunner(true).queryMetrics(context.Background(), cache)
	must.NoError(t, err)

	// Checks that differ only in QueryInstant must not share a cache entry.
	must.Eq(t, 2, looker.calls)
	must.Eq(t, sdk.TimeRange{From: ts(-6, 0), To: ts(-1, 0)}, looker.ranges[0])
	must.Eq(t, sdk.TimeRange{From: ts(-1, 0), To: ts(-1, 0)}, looker.ranges[1])
	must.Eq(t, rangeMetrics, gotRange)
	must.Eq(t, instantMetrics, gotInstant)
}

func TestCheckHandler_queryMetricsCache_SufficiencyRulesNotShared(t *testing.T) {
	fixNow(t)

	metrics := sdk.TimestampedMetrics{{Timestamp: ts(-2, 0), Value: 1}}
	looker := &fakeAPMLooker{results: []fakeAPMResult{{metrics: metrics}}}
	p := &sdk.ScalingPolicy{ID: "testPolicy"}
	cache := newQueryMetricsCache()

	mkRunner := func(minPoints int) *checkRunner {
		return newTestCheckRunner(looker, &sdk.ScalingPolicyCheck{
			Name:              "check",
			Source:            "mock",
			Query:             "query",
			QueryWindow:       5 * time.Minute,
			QueryWindowOffset: time.Minute,
			QueryMinPoints:    minPoints,
		}, p)
	}

	// The first check requires more points than the query returns; the
	// second shares the cached raw result but applies its own, looser limit.
	_, err := mkRunner(5).queryMetrics(context.Background(), cache)
	must.ErrorIs(t, err, errInsufficientMetrics)

	got, err := mkRunner(1).queryMetrics(context.Background(), cache)
	must.NoError(t, err)
	must.Eq(t, metrics, got)

	// The APM was queried only once; the sufficiency failure was not cached.
	must.Eq(t, 1, looker.calls)
}

func TestCheckHandler_runCheckAndCapCount_MetricErrorHandling(t *testing.T) {
	fixNow(t)

	onePoint := sdk.TimestampedMetrics{{Timestamp: ts(-2, 0), Value: 1}}

	testCases := []struct {
		name             string
		lookerResults    []fakeAPMResult
		minPoints        int
		checkOnError     string
		policyOnError    string
		wantSkipped      bool
		wantStrategyRuns int
	}{
		// Query failure.
		{name: "query_error_ignore", lookerResults: []fakeAPMResult{{err: errTest}},
			checkOnError: sdk.ScalingPolicyOnErrorIgnore, wantSkipped: true},
		{name: "query_error_fail", lookerResults: []fakeAPMResult{{err: errTest}},
			checkOnError: sdk.ScalingPolicyOnErrorFail, wantSkipped: false},
		{name: "query_error_unset_policy_fail", lookerResults: []fakeAPMResult{{err: errTest}},
			policyOnError: sdk.ScalingPolicyOnErrorFail, wantSkipped: false},
		{name: "query_error_unset_policy_ignore", lookerResults: []fakeAPMResult{{err: errTest}},
			policyOnError: sdk.ScalingPolicyOnErrorIgnore, wantSkipped: true},
		{name: "query_error_unset_policy_unset", lookerResults: []fakeAPMResult{{err: errTest}},
			wantSkipped: true},

		// Empty result.
		{name: "empty_ignore", lookerResults: []fakeAPMResult{{metrics: sdk.TimestampedMetrics{}}},
			checkOnError: sdk.ScalingPolicyOnErrorIgnore, wantSkipped: true},
		{name: "empty_fail", lookerResults: []fakeAPMResult{{metrics: sdk.TimestampedMetrics{}}},
			checkOnError: sdk.ScalingPolicyOnErrorFail, wantSkipped: false},
		{name: "empty_unset_policy_fail", lookerResults: []fakeAPMResult{{metrics: sdk.TimestampedMetrics{}}},
			policyOnError: sdk.ScalingPolicyOnErrorFail, wantSkipped: false},
		{name: "empty_unset_policy_ignore", lookerResults: []fakeAPMResult{{metrics: sdk.TimestampedMetrics{}}},
			policyOnError: sdk.ScalingPolicyOnErrorIgnore, wantSkipped: true},
		{name: "empty_unset_policy_unset", lookerResults: []fakeAPMResult{{metrics: sdk.TimestampedMetrics{}}},
			wantSkipped: true},

		// Insufficient data.
		{name: "insufficient_ignore", lookerResults: []fakeAPMResult{{metrics: onePoint}}, minPoints: 2,
			checkOnError: sdk.ScalingPolicyOnErrorIgnore, wantSkipped: true},
		{name: "insufficient_fail", lookerResults: []fakeAPMResult{{metrics: onePoint}}, minPoints: 2,
			checkOnError: sdk.ScalingPolicyOnErrorFail, wantSkipped: false},
		{name: "insufficient_unset_policy_fail", lookerResults: []fakeAPMResult{{metrics: onePoint}}, minPoints: 2,
			policyOnError: sdk.ScalingPolicyOnErrorFail, wantSkipped: false},
		{name: "insufficient_unset_policy_ignore", lookerResults: []fakeAPMResult{{metrics: onePoint}}, minPoints: 2,
			policyOnError: sdk.ScalingPolicyOnErrorIgnore, wantSkipped: true},
		{name: "insufficient_unset_policy_unset", lookerResults: []fakeAPMResult{{metrics: onePoint}}, minPoints: 2,
			wantSkipped: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looker := &fakeAPMLooker{results: tc.lookerResults}
			sr := &mockStrategyRunner{t: t}

			p := &sdk.ScalingPolicy{ID: "testPolicy", OnCheckError: tc.policyOnError}

			runner := newCheckRunner(&CheckRunnerConfig{
				Log:            hclog.NewNullLogger(),
				StrategyRunner: sr,
				MetricsGetter:  looker,
				Policy:         p,
			}, &sdk.ScalingPolicyCheck{
				Name:              "check",
				Source:            "mock",
				Query:             "query",
				QueryWindow:       5 * time.Minute,
				QueryWindowOffset: time.Minute,
				QueryMinPoints:    tc.minPoints,
				OnError:           tc.checkOnError,
				Strategy:          &sdk.ScalingPolicyStrategy{Name: "strategy"},
			})

			action, err := runner.runCheckAndCapCount(context.Background(), 5, newQueryMetricsCache())
			if tc.wantSkipped {
				must.ErrorIs(t, err, errCheckSkipped)
				must.Eq(t, sdk.ScalingAction{}, action)
			} else {
				must.Error(t, err)
				must.False(t, errors.Is(err, errCheckSkipped))
			}

			// The strategy must never run when the metric query failed.
			must.Eq(t, tc.wantStrategyRuns, sr.runCalls)
		})
	}
}

func Test_metricErrorType(t *testing.T) {
	must.Eq(t, "query_error", metricErrorType(fmt.Errorf("%w: boom", errMetricsQueryFailed)))
	must.Eq(t, "query_error", metricErrorType(errTest))
	must.Eq(t, "empty_result", metricErrorType(errNoMetrics))
	must.Eq(t, "insufficient_data", metricErrorType(
		fmt.Errorf("%w: 1 valid data points, minimum is 2", errInsufficientMetrics)))
}
