/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// SampleAndDeleteTimeoutDuration stops re-sampling at multi-node consolidation's time limit. It's a var so tests can
// shorten it.
var SampleAndDeleteTimeoutDuration = MultiNodeConsolidationTimeoutDuration

// SampleAndDelete consolidates several nodes at once by random sampling. Each round keeps every candidate with its
// delete probability, which is high for lightly used nodes that are cheap to disrupt, then simulates removing the kept
// nodes and accepts them when Balanced scoring finds the savings justify the disruption. A rejected sample is redrawn
// until the time limit.
type SampleAndDelete struct {
	consolidation
	validator      Validator
	rng            *rand.Rand
	nodePoolTotals map[string]NodePoolTotals
}

func NewSampleAndDelete(c consolidation, opts ...option.Function[MethodOptions]) *SampleAndDelete {
	o := option.Resolve(append([]option.Function[MethodOptions]{
		WithValidator(NewSampleAndDeleteValidator(c)),
		WithRand(rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))), //nolint:gosec // sampling needs no cryptographic randomness
	}, opts...)...)
	// The series starts at zero only when the feature gate builds this method.
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: SampleAndDeleteConsolidationType.Name})
	return &SampleAndDelete{
		consolidation: c,
		validator:     o.validator,
		rng:           o.rng,
	}
}

// SetNodePoolTotals keeps the totals, since sample-and-delete scores pools of every policy and not only Balanced ones.
func (s *SampleAndDelete) SetNodePoolTotals(totals map[string]NodePoolTotals) {
	s.consolidation.SetNodePoolTotals(totals)
	s.nodePoolTotals = totals
}

func (s *SampleAndDelete) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}
	// Sorting like multi-node consolidation makes budgets keep the same candidates. Names break ties so a seed repeats.
	slices.SortFunc(candidates, func(a, b *Candidate) int {
		return cmp.Or(cmp.Compare(b.SavingsRatio(), a.SavingsRatio()), strings.Compare(a.Name(), b.Name()))
	})
	candidates, constrainedByBudgets := filterByBudget(candidates, disruptionBudgetMapping)

	cmd, perPool, err := s.sampleUntilAccepted(ctx, candidates)
	if err != nil {
		return []Command{}, err
	}
	if cmd.Decision() == NoOpDecision {
		// A budget can free up before the cluster changes, so only mark consolidated when no budget held a candidate back.
		if !constrainedByBudgets {
			s.markConsolidated()
		}
		return []Command{}, nil
	}
	s.evaluator.EmitMultiNodeEvents(ctx, cmd, perPool, true)

	if _, err = s.validator.Validate(ctx, cmd, commandValidationDelay); err != nil {
		if IsValidationError(err) {
			cmd.EmitRejectedEvents(s.recorder, getValidationFailureReason(err))
			return []Command{}, nil
		}
		return []Command{}, fmt.Errorf("validating consolidation, %w", err)
	}
	return []Command{cmd}, nil
}

// sampleUntilAccepted draws samples until Balanced scoring accepts one or the time limit passes, and returns an empty
// command when none is accepted.
func (s *SampleAndDelete) sampleUntilAccepted(ctx context.Context, candidates []*Candidate) (Command, map[string]ScoreResult, error) {
	// A sample always holds at least two nodes, and single-node consolidation handles one.
	if len(candidates) < 2 {
		return Command{}, nil, nil
	}
	probabilities := s.DeleteProbabilities(ctx, candidates)

	// The limit is checked between samples, as single-node consolidation does, since a context deadline makes the API
	// client fail calls early when it expects them to run past it.
	deadline := s.clock.Now().Add(SampleAndDeleteTimeoutDuration)
	// Samples are scored without logging, since a debug line for each one would flood the log.
	quietCtx := log.IntoContext(ctx, operatorlogging.NopLogger)
	var lastRejected Command
	var lastRejectedPerPool map[string]ScoreResult
	for samples := 0; ; samples++ {
		if !s.clock.Now().Before(deadline) {
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			log.FromContext(ctx).V(1).Info("stopping sample-and-delete consolidation after timeout", "samples", samples, "last_rejected_scores", lastRejectedPerPool)
			break
		}
		sample := drawSample(s.rng, candidates, probabilities)
		cmd, err := s.computeConsolidation(ctx, sample...)
		if err != nil {
			return Command{}, nil, err
		}
		// A sample whose pods can't all move is rejected like one that saves too little.
		if cmd.Decision() == NoOpDecision {
			continue
		}
		if cmd.Decision() == ReplaceDecision {
			// A replacement of a type the sample already runs costs more than deleting only part of the sample.
			replacement, err := filterOutSameInstanceType(cmd.Replacements[0], sample)
			if err != nil || len(replacement.InstanceTypeOptions) == 0 {
				continue
			}
			cmd.Replacements[0] = replacement
		}
		approved, perPool := EvaluateMoveForAllPools(quietCtx, cmd, s.nodePoolTotals)
		if approved {
			log.FromContext(ctx).V(1).WithValues(cmd.LogValues()...).Info("found sample-and-delete consolidation", "samples", samples+1, "scores", perPool)
			return cmd, perPool, nil
		}
		lastRejected, lastRejectedPerPool = cmd, perPool
	}
	// Report one rejection per search rather than one per sample.
	if lastRejectedPerPool != nil {
		s.evaluator.EmitMultiNodeEvents(ctx, lastRejected, lastRejectedPerPool, false)
	}
	return Command{}, nil, nil
}

// DeleteProbabilities returns the chance that a sample keeps each candidate, in the order given. Disruption cost is
// scaled by the highest among the candidates, so the result depends on which candidates are passed together.
func (s *SampleAndDelete) DeleteProbabilities(ctx context.Context, candidates []*Candidate) []float64 {
	weights := options.FromContext(ctx).SampleAndDeleteWeights
	repairEnabled := options.FromContext(ctx).FeatureGates.NodeRepair && !options.FromContext(ctx).LegacyNodeRepair
	// All candidates share one now, so their ages are measured at the same moment.
	now := s.clock.Now()
	fixedNowClk := fixedNowClock{Clock: s.clock, now: now}
	maxDisruptionCost := lo.Max(lo.Map(candidates, func(c *Candidate, _ int) float64 { return c.RescheduleDisruptionCost }))
	return lo.Map(candidates, func(c *Candidate, _ int) float64 {
		return deleteProbability(weights, deletionTerms{
			Utilization:    utilization(c),
			DisruptionCost: lo.Ternary(maxDisruptionCost > 0, c.RescheduleDisruptionCost/maxDisruptionCost, 0),
			Drift:          lo.Ternary(c.NodeClaim.StatusConditions().IsTrue(v1.ConditionTypeDrifted), 0.0, 1.0),
			Repair:         lo.Ternary(repairEnabled && c.GetRepairResult(now).Action != "", 0.0, 1.0),
			Expiration:     lifetimeRemaining(fixedNowClk, c),
		})
	})
}

// deletionTerms are the inputs to a candidate's delete probability. Each is between 0 and 1, and a higher value makes
// deleting the node less likely.
type deletionTerms struct {
	// Utilization is how full the node is.
	Utilization float64
	// DisruptionCost is the node's disruption cost as a share of the highest among the candidates.
	DisruptionCost float64
	// Drift is 0 when the NodeClaim is drifted and 1 otherwise.
	Drift float64
	// Repair is 0 when node repair wants to fix the node and 1 otherwise.
	Repair float64
	// Expiration is the share of the node's lifetime left, and 1 when it never expires.
	Expiration float64
}

// deleteProbability is 1 minus the weighted average of the terms.
func deleteProbability(w options.SampleAndDeleteWeights, t deletionTerms) float64 {
	return lo.Clamp(1-(w.Utilization*t.Utilization+w.DisruptionCost*t.DisruptionCost+w.Drift*t.Drift+w.Repair*t.Repair+w.Expiration*t.Expiration), 0, 1)
}

// utilization is the larger of the CPU and memory shares of allocatable that the node's reschedulable pods request.
// DaemonSet pods aren't reschedulable, so they don't count: they don't move when the node goes away.
func utilization(c *Candidate) float64 {
	requests := resources.RequestsForPods(c.reschedulablePods...)
	allocatable := c.Allocatable()
	highest := 0.0
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		total, ok := allocatable[name]
		if !ok || total.IsZero() {
			continue
		}
		request := requests[name]
		highest = math.Max(highest, request.AsApproximateFloat64()/total.AsApproximateFloat64())
	}
	return lo.Clamp(highest, 0, 1)
}

// lifetimeRemaining is 1 - age/expireAfter between 0 and 1, and 1 when the NodeClaim never expires.
func lifetimeRemaining(clk clock.Clock, c *Candidate) float64 {
	remaining := disruptionutils.LifetimeRemaining(clk, c.NodePool, c.NodeClaim)
	// An expireAfter of zero gives 0/0 for a brand new node, which has no lifetime left either.
	if math.IsNaN(remaining) {
		return 0
	}
	return remaining
}

// drawSample keeps each candidate with its own probability. When it keeps fewer than two, it takes the two most
// likely candidates instead.
func drawSample(rng *rand.Rand, candidates []*Candidate, probabilities []float64) []*Candidate {
	var sample []*Candidate
	for i, c := range candidates {
		if rng.Float64() < probabilities[i] {
			sample = append(sample, c)
		}
	}
	if len(sample) >= 2 {
		return sample
	}
	return mostLikely(candidates, probabilities, 2)
}

// mostLikely returns the n candidates with the highest delete probability. Names break ties so the pick repeats.
func mostLikely(candidates []*Candidate, probabilities []float64, n int) []*Candidate {
	order := lo.Range(len(candidates))
	slices.SortFunc(order, func(i, j int) int {
		return cmp.Or(cmp.Compare(probabilities[j], probabilities[i]), strings.Compare(candidates[i].Name(), candidates[j].Name()))
	})
	return lo.Map(order[:min(n, len(order))], func(i int, _ int) *Candidate { return candidates[i] })
}

func (s *SampleAndDelete) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *SampleAndDelete) Class() string {
	return GracefulDisruptionClass
}

func (s *SampleAndDelete) ConsolidationType() string {
	return SampleAndDeleteConsolidationType.Name
}
