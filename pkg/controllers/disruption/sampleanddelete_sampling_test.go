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

// Tests in this file exercise sample-and-delete's probability and sampling functions with hand-built candidates.
// Tests that run scheduling simulation against cluster state are in sampleanddelete_test.go.

package disruption

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// podRequesting returns a pod with one container that requests cpu and memory.
func podRequesting(name, cpu, memory string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
			}},
		}}},
	}
}

// seededRand returns a random source that repeats for a seed.
func seededRand(seed1, seed2 uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed1, seed2)) //nolint:gosec // tests need a repeatable source, not a secure one
}

func candidateNames(candidates []*Candidate) []string {
	return lo.Map(candidates, func(c *Candidate, _ int) string { return c.Name() })
}

var _ = Describe("Sample-And-Delete Sampling", func() {
	var np *v1.NodePool
	BeforeEach(func() {
		np = makeNodePool("pool", v1.ConsolidationPolicyWhenEmptyOrUnderutilized)
	})

	Describe("deleteProbability", func() {
		DescribeTable("should be 1 minus the weighted average of the terms",
			func(weights options.SampleAndDeleteWeights, terms deletionTerms, expected float64) {
				Expect(deleteProbability(weights, terms)).To(BeNumerically("~", expected, 1e-9))
			},
			// The default weights give the base formula, 1 - (U + DC) / 2.
			Entry("base formula", options.DefaultSampleAndDeleteWeights,
				deletionTerms{Utilization: 0.2, DisruptionCost: 0.6, Drift: 1, Repair: 1, Expiration: 1}, 0.6),
			Entry("base formula for an empty node that is cheap to disrupt", options.DefaultSampleAndDeleteWeights,
				deletionTerms{}, 1.0),
			Entry("base formula for a full node that is costly to disrupt", options.DefaultSampleAndDeleteWeights,
				deletionTerms{Utilization: 1, DisruptionCost: 1}, 0.0),
			// Extension 1 tunes the weights of utilization and disruption cost.
			Entry("utilization weighted more than disruption cost", options.SampleAndDeleteWeights{Utilization: 0.8, DisruptionCost: 0.2},
				deletionTerms{Utilization: 0.5, DisruptionCost: 1}, 0.4),
			// Extension 2 adds drift, repair and expiration terms.
			Entry("a drifted node", options.SampleAndDeleteWeights{Utilization: 0.5, Drift: 0.5},
				deletionTerms{Utilization: 0.4, Drift: 0}, 0.8),
			Entry("a node that isn't drifted", options.SampleAndDeleteWeights{Utilization: 0.5, Drift: 0.5},
				deletionTerms{Utilization: 0.4, Drift: 1}, 0.3),
			Entry("a node that needs repair", options.SampleAndDeleteWeights{Utilization: 0.5, Repair: 0.5},
				deletionTerms{Utilization: 0.4, Repair: 0}, 0.8),
			Entry("a healthy node", options.SampleAndDeleteWeights{Utilization: 0.5, Repair: 0.5},
				deletionTerms{Utilization: 0.4, Repair: 1}, 0.3),
			Entry("a node with a quarter of its lifetime left", options.SampleAndDeleteWeights{Utilization: 0.5, Expiration: 0.5},
				deletionTerms{Utilization: 0.4, Expiration: 0.25}, 0.675),
			Entry("a node that never expires", options.SampleAndDeleteWeights{Utilization: 0.5, Expiration: 0.5},
				deletionTerms{Utilization: 0.4, Expiration: 1}, 0.3),
			Entry("all five terms", options.SampleAndDeleteWeights{Utilization: 0.2, DisruptionCost: 0.2, Drift: 0.2, Repair: 0.2, Expiration: 0.2},
				deletionTerms{Utilization: 0.5, DisruptionCost: 0.5, Drift: 0, Repair: 1, Expiration: 0.5}, 0.5),
		)
	})

	Describe("utilization", func() {
		candidateWith := func(allocatable corev1.ResourceList, pods ...*corev1.Pod) *Candidate {
			c := makeCandidate("node", np, makeInstanceType("it", 1.0), pods)
			c.Node.Status.Allocatable = allocatable
			return c
		}
		allocatable := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")}

		It("should use CPU when CPU is the most requested resource", func() {
			Expect(utilization(candidateWith(allocatable, podRequesting("a", "2", "1Gi")))).To(BeNumerically("~", 0.25, 1e-9))
		})
		It("should use memory when memory is the most requested resource", func() {
			Expect(utilization(candidateWith(allocatable, podRequesting("a", "1", "12Gi")))).To(BeNumerically("~", 0.75, 1e-9))
		})
		It("should add up the requests of every reschedulable pod", func() {
			Expect(utilization(candidateWith(allocatable, podRequesting("a", "1", "1Gi"), podRequesting("b", "3", "1Gi")))).To(BeNumerically("~", 0.5, 1e-9))
		})
		It("should be at most 1 when pods request more than the node has", func() {
			Expect(utilization(candidateWith(allocatable, podRequesting("a", "10", "1Gi")))).To(Equal(1.0))
		})
		It("should skip a resource the node doesn't report", func() {
			cpuOnly := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}
			Expect(utilization(candidateWith(cpuOnly, podRequesting("a", "2", "64Gi")))).To(BeNumerically("~", 0.25, 1e-9))
		})
		It("should be 0 without reschedulable pods", func() {
			Expect(utilization(candidateWith(allocatable))).To(Equal(0.0))
		})
	})

	Describe("DeleteProbabilities", func() {
		var s *SampleAndDelete
		var clk *clocktesting.FakeClock
		var ctx context.Context

		withWeights := func(weights options.SampleAndDeleteWeights) context.Context {
			return options.ToContext(context.Background(), &options.Options{SampleAndDeleteWeights: weights})
		}
		// managedCandidate has a NodeClaim created at the fake clock's now and a node with 8 CPUs and 16Gi.
		managedCandidate := func(name string, pods ...*corev1.Pod) *Candidate {
			c := makeCandidate(name, np, makeInstanceType("it", 1.0), pods)
			c.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(clk.Now())}}
			c.Node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")}
			return c
		}

		BeforeEach(func() {
			clk = clocktesting.NewFakeClock(time.Now())
			s = &SampleAndDelete{consolidation: consolidation{clock: clk}}
			ctx = withWeights(options.DefaultSampleAndDeleteWeights)
		})

		It("should give the base formula with the default weights", func() {
			// node-a: U = 0.25 and DC = 2/3. node-b: U = 0.5 and DC = 1.
			a := managedCandidate("node-a", podRequesting("a", "2", "1Gi"))
			b := managedCandidate("node-b", podRequesting("b1", "2", "1Gi"), podRequesting("b2", "2", "1Gi"))
			Expect(s.DeleteProbabilities(ctx, []*Candidate{a, b})).To(HaveExactElements(
				BeNumerically("~", 1-(0.25+2.0/3)/2, 1e-9),
				BeNumerically("~", 1-(0.5+1.0)/2, 1e-9),
			))
		})
		It("should scale disruption cost by the highest among the candidates", func() {
			ctx = withWeights(options.SampleAndDeleteWeights{DisruptionCost: 1})
			// Disruption costs are 2, 3 and 5: one per node plus one per pod.
			a := managedCandidate("node-a", podRequesting("a", "1", "1Gi"))
			b := managedCandidate("node-b", podRequesting("b1", "1", "1Gi"), podRequesting("b2", "1", "1Gi"))
			c := managedCandidate("node-c", lo.Times(4, func(i int) *corev1.Pod { return podRequesting(fmt.Sprintf("c%d", i), "1", "1Gi") })...)
			Expect(s.DeleteProbabilities(ctx, []*Candidate{a, b, c})).To(HaveExactElements(
				BeNumerically("~", 1-2.0/5, 1e-9),
				BeNumerically("~", 1-3.0/5, 1e-9),
				BeNumerically("~", 0.0, 1e-9),
			))
			// Without node-c the highest cost is 3, so node-a's probability changes.
			Expect(s.DeleteProbabilities(ctx, []*Candidate{a, b})).To(HaveExactElements(
				BeNumerically("~", 1-2.0/3, 1e-9),
				BeNumerically("~", 0.0, 1e-9),
			))
		})
		It("should make drifted nodes certain to be kept when only drift counts", func() {
			ctx = withWeights(options.SampleAndDeleteWeights{Drift: 1})
			drifted := managedCandidate("drifted", podRequesting("a", "1", "1Gi"))
			drifted.NodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			notDrifted := managedCandidate("not-drifted", podRequesting("b", "1", "1Gi"))
			Expect(s.DeleteProbabilities(ctx, []*Candidate{drifted, notDrifted})).To(HaveExactElements(1.0, 0.0))
		})
		It("should make nodes more likely to be kept as they near expiration", func() {
			ctx = withWeights(options.SampleAndDeleteWeights{Expiration: 1})
			quarterLeft := managedCandidate("quarter-left", podRequesting("a", "1", "1Gi"))
			quarterLeft.NodeClaim.Spec.ExpireAfter = v1.MustParseNillableDuration("2h")
			never := managedCandidate("never", podRequesting("b", "1", "1Gi"))
			never.NodeClaim.Spec.ExpireAfter = v1.MustParseNillableDuration("Never")
			expired := managedCandidate("expired", podRequesting("c", "1", "1Gi"))
			expired.NodeClaim.Spec.ExpireAfter = v1.MustParseNillableDuration("1h")
			clk.Step(90 * time.Minute)
			// quarter-left has 30 of its 120 minutes left, so E = 0.25. A node that never expires has E = 1.
			Expect(s.DeleteProbabilities(ctx, []*Candidate{quarterLeft, never, expired})).To(HaveExactElements(
				BeNumerically("~", 0.75, 1e-9),
				0.0,
				1.0,
			))
		})
		It("should not count a node as needing repair while node repair is disabled", func() {
			ctx = withWeights(options.SampleAndDeleteWeights{Repair: 1})
			Expect(s.DeleteProbabilities(ctx, []*Candidate{managedCandidate("a"), managedCandidate("b")})).To(HaveExactElements(0.0, 0.0))
		})
	})

	Describe("drawSample", func() {
		var candidates []*Candidate
		BeforeEach(func() {
			candidates = lo.Times(6, func(i int) *Candidate {
				return makeCandidate(fmt.Sprintf("node-%d", i), np, makeInstanceType("it", 1.0), []*corev1.Pod{makePod(fmt.Sprintf("pod-%d", i), "")})
			})
		})

		It("should draw the same samples from the same seed", func() {
			probabilities := []float64{0.9, 0.7, 0.5, 0.5, 0.3, 0.1}
			a, b := seededRand(1, 2), seededRand(1, 2)
			for range 100 {
				Expect(candidateNames(drawSample(a, candidates, probabilities))).To(Equal(candidateNames(drawSample(b, candidates, probabilities))))
			}
		})
		It("should draw different samples from a different seed", func() {
			probabilities := []float64{0.9, 0.7, 0.5, 0.5, 0.3, 0.1}
			a, b := seededRand(1, 2), seededRand(3, 4)
			differs := false
			for range 100 {
				differs = differs || fmt.Sprint(candidateNames(drawSample(a, candidates, probabilities))) != fmt.Sprint(candidateNames(drawSample(b, candidates, probabilities)))
			}
			Expect(differs).To(BeTrue())
		})
		It("should keep each candidate about as often as its probability", func() {
			// The two certain candidates put at least two nodes in every sample, so the fallback never applies.
			probabilities := []float64{1, 1, 0.25, 0.5, 0.75, 0}
			rng := seededRand(5, 6)
			const draws = 20000
			kept := map[string]int{}
			for range draws {
				for _, c := range drawSample(rng, candidates, probabilities) {
					kept[c.Name()]++
				}
			}
			for i, c := range candidates {
				Expect(float64(kept[c.Name()])/draws).To(BeNumerically("~", probabilities[i], 0.02), c.Name())
			}
		})
		It("should take the two most likely candidates when it keeps none", func() {
			probabilities := []float64{1e-12, 3e-12, 2e-12, 0, 0, 0}
			rng := seededRand(1, 2)
			for range 100 {
				Expect(candidateNames(drawSample(rng, candidates, probabilities))).To(Equal([]string{"node-1", "node-2"}))
			}
		})
		It("should take the two most likely candidates when it keeps only one", func() {
			// node-4 is always kept and node-1 almost never, so the fallback adds node-1 as the next most likely.
			probabilities := []float64{0, 1e-12, 0, 0, 1, 0}
			rng := seededRand(1, 2)
			for range 100 {
				Expect(candidateNames(drawSample(rng, candidates, probabilities))).To(Equal([]string{"node-4", "node-1"}))
			}
		})
		It("should never return fewer than two candidates", func() {
			probabilities := []float64{0.3, 0.1, 0.2, 0.05, 0.15, 0.1}
			rng := seededRand(9, 9)
			for range 1000 {
				Expect(len(drawSample(rng, candidates, probabilities))).To(BeNumerically(">=", 2))
			}
		})
	})

	Describe("mostLikely", func() {
		It("should order by probability and break ties by name", func() {
			candidates := lo.Map([]string{"c", "a", "b", "d"}, func(name string, _ int) *Candidate {
				return makeCandidate(name, np, makeInstanceType("it", 1.0), []*corev1.Pod{makePod("pod-"+name, "")})
			})
			Expect(candidateNames(mostLikely(candidates, []float64{0.5, 0.5, 0.9, 0.1}, 3))).To(Equal([]string{"b", "a", "c"}))
		})
	})
})
