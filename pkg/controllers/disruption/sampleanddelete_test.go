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

package disruption_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// countingSource counts the random numbers drawn, which gives the number of samples tried.
type countingSource struct {
	rand.Source
	draws atomic.Int64
}

func (s *countingSource) Uint64() uint64 {
	s.draws.Add(1)
	return s.Source.Uint64()
}

var _ = Describe("Sample-And-Delete Consolidation", func() {
	var nodePool *v1.NodePool
	var small *cloudprovider.InstanceType

	// onDemandInstanceType is an instance type with one on-demand offering at the given price.
	onDemandInstanceType := func(name string, cpu string, price float64) *cloudprovider.InstanceType {
		return fake.NewInstanceType(name,
			fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourcePods: resource.MustParse("10")}),
			fake.WithOfferings(cloudprovider.Offering{
				Available:    true,
				Compatible:   true,
				Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1"}),
				Price:        price,
			}),
		)
	}
	// useInstanceTypes offers a 4-CPU type at price 1 and an 8-CPU type at largePrice.
	useInstanceTypes := func(largePrice float64) {
		small = onDemandInstanceType("small", "4", 1.0)
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{small, onDemandInstanceType("large", "8", largePrice)}
		ExpectSingletonReconciled(ctx, pricingController)
	}
	// makeNodes creates initialized small nodes in the NodePool that each run one ReplicaSet pod requesting podCPU.
	makeNodes := func(count int, np *v1.NodePool, podCPU string) ([]*v1.NodeClaim, []*corev1.Node) {
		GinkgoHelper()
		nodeClaims, nodes := test.NodeClaimsAndNodes(count, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            np.Name,
				corev1.LabelInstanceTypeStable: small.Name,
				v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone:       "test-zone-1",
			}},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("10")}},
		})
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs, np)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
		for i := range nodeClaims {
			nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: new(true), BlockOwnerDeletion: new(true),
				}}},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(podCPU)}},
			})
			ExpectApplied(ctx, env.Client, pod, nodeClaims[i], nodes[i])
			ExpectManualBinding(ctx, env.Client, pod, nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		return nodeClaims, nodes
	}
	// newSampleAndDelete builds the method around a seeded random source, without the validation delay.
	newSampleAndDelete := func(src rand.Source) *disruption.SampleAndDelete {
		return disruption.NewSampleAndDelete(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
			disruption.WithValidator(NopValidator{}), disruption.WithRand(rand.New(src))) //nolint:gosec // tests need a repeatable source, not a secure one
	}
	// computeCommands feeds the method candidates, totals and budgets the way the disruption controller does.
	computeCommands := func(s *disruption.SampleAndDelete) []disruption.Command {
		GinkgoHelper()
		candidates, totals, err := disruption.GetCandidatesWithTotals(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, s.ShouldDisrupt, s.Class(), queue, clusterCost)
		Expect(err).ToNot(HaveOccurred())
		s.SetNodePoolTotals(totals)
		budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, env.Clock, env.Client, cloudProvider, recorder, s.Reason())
		Expect(err).ToNot(HaveOccurred())
		cmds, err := s.ComputeCommands(ctx, budgets, candidates...)
		Expect(err).ToNot(HaveOccurred())
		return cmds
	}
	// computeUntilTimeLimit lets the method draw a few samples, then moves the clock past its time limit.
	computeUntilTimeLimit := func(s *disruption.SampleAndDelete, src *countingSource) []disruption.Command {
		GinkgoHelper()
		result := make(chan []disruption.Command, 1)
		go func() {
			defer GinkgoRecover()
			result <- computeCommands(s)
		}()
		Eventually(src.draws.Load, 10*time.Second).Should(BeNumerically(">=", 10))
		env.Clock.Step(disruption.SampleAndDeleteTimeoutDuration)
		var cmds []disruption.Command
		Eventually(result, 10*time.Second).Should(Receive(&cmds))
		return cmds
	}
	candidateNamesByPool := func(cmd disruption.Command) map[string][]string {
		return lo.MapValues(lo.GroupBy(cmd.Candidates, func(c *disruption.Candidate) string { return c.NodePool.Name }),
			func(cs []*disruption.Candidate, _ string) []string {
				return lo.Map(cs, func(c *disruption.Candidate, _ int) string { return c.Name() })
			})
	}
	newNodePool := func(budget string) *v1.NodePool {
		return test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
			Budgets:             []v1.Budget{{Nodes: budget}},
		}}})
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{SampleAndDeleteConsolidation: new(true)}}))
		nodePool = newNodePool("100%")
		useInstanceTypes(2.0)
		disruption.ConsolidationTimeoutsTotal.Reset()
	})
	AfterEach(func() {
		disruption.SampleAndDeleteTimeoutDuration = disruption.MultiNodeConsolidationTimeoutDuration
	})

	Context("Feature Gate", func() {
		methodTypes := func(methods []disruption.Method) []string {
			return lo.Map(methods, func(m disruption.Method, _ int) string { return fmt.Sprintf("%T", m) })
		}
		It("should keep multi-node consolidation when the gate is off", func() {
			methods := disruption.NewMethods(options.ToContext(ctx, test.Options()), env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			Expect(methodTypes(methods)).To(Equal([]string{
				"*disruption.Emptiness", "*disruption.StaticDrift", "*disruption.Drift", "*disruption.MultiNodeConsolidation", "*disruption.SingleNodeConsolidation",
			}))
		})
		It("should put sample-and-delete in multi-node consolidation's place when the gate is on", func() {
			methods := disruption.NewMethods(ctx, env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
			Expect(methodTypes(methods)).To(Equal([]string{
				"*disruption.Emptiness", "*disruption.StaticDrift", "*disruption.Drift", "*disruption.SampleAndDelete", "*disruption.SingleNodeConsolidation",
			}))
		})
	})

	Context("Acceptance", func() {
		It("should accept a sample when Balanced scoring approves the savings", func() {
			// Two 3-CPU pods only fit together on an 8-CPU node. At price 0.9 it saves 1.1 of the pool's 2.0 while
			// disrupting the whole pool, a score of 0.55 against the threshold of 0.5.
			useInstanceTypes(0.9)
			_, nodes := makeNodes(2, nodePool, "3")
			cmds := computeCommands(newSampleAndDelete(rand.NewPCG(1, 1)))
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(candidateNamesByPool(cmds[0])[nodePool.Name]).To(ConsistOf(nodes[0].Name, nodes[1].Name))
			Expect(cmds[0].Replacements[0].InstanceTypeOptions).To(HaveLen(1))
			Expect(cmds[0].Replacements[0].InstanceTypeOptions[0].Name).To(Equal("large"))
			ExpectMetricCounterValue(disruption.ConsolidationMovesTotal, 1, map[string]string{
				"decision": string(disruption.ApprovedDecision), metrics.NodePoolLabel: nodePool.Name, "policy": string(v1.ConsolidationPolicyWhenEmptyOrUnderutilized),
			})
		})
		It("should reject a sample when Balanced scoring finds the savings too small, even outside Balanced NodePools", func() {
			// At price 1.95 the same move saves 0.05 of 2.0, a score of 0.025.
			useInstanceTypes(1.95)
			makeNodes(2, nodePool, "3")
			src := &countingSource{Source: rand.NewPCG(1, 1)}
			Expect(computeUntilTimeLimit(newSampleAndDelete(src), src)).To(BeEmpty())
			// The rejection is reported once for the whole search.
			ExpectMetricCounterValue(disruption.ConsolidationMovesTotal, 1, map[string]string{
				"decision": string(disruption.RejectedDecision), metrics.NodePoolLabel: nodePool.Name, "policy": string(v1.ConsolidationPolicyWhenEmptyOrUnderutilized),
			})
		})
		It("should record its decisions under its own consolidation type", func() {
			makeNodes(3, nodePool, "1")
			disruptionController = disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
				disruption.WithMethods(newSampleAndDelete(rand.NewPCG(1, 1))))
			ExpectSingletonReconciled(ctx, disruptionController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].ConsolidationType()).To(Equal(disruption.SampleAndDeleteConsolidationType.Name))
			Expect(cmds[0].Reason()).To(Equal(v1.DisruptionReasonUnderutilized))
			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 1, map[string]string{
				"decision": string(cmds[0].Decision()), metrics.ReasonLabel: "underutilized", "consolidation_type": "sample-and-delete",
			})
			ExpectMetricCounterValue(disruption.NodepoolDecisionsPerformed, 1, map[string]string{
				metrics.NodePoolLabel: nodePool.Name, "decision": string(cmds[0].Decision()), metrics.ReasonLabel: "underutilized", "consolidation_type": "sample-and-delete",
			})
		})
		It("should wait before acting and re-check the sample", func() {
			makeNodes(3, nodePool, "1")
			s := disruption.NewSampleAndDelete(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue),
				disruption.WithRand(rand.New(rand.NewPCG(1, 1)))) //nolint:gosec // tests need a repeatable source, not a secure one
			disruptionController = disruption.NewController(ctx, env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(s))

			finished := atomic.Bool{}
			ExpectParallelized(
				func() {
					defer finished.Store(true)
					ExpectSingletonReconciled(ctx, disruptionController)
				},
				func() {
					// The controller blocks on the validation delay before it starts the command.
					Eventually(env.Clock.HasWaiters, time.Second*10).Should(BeTrue())
					Expect(finished.Load()).To(BeFalse())
					Expect(queue.GetCommands()).To(BeEmpty())
					env.Clock.Step(16 * time.Second)
					Eventually(finished.Load, time.Second*10).Should(BeTrue())
				},
			)
			Expect(queue.GetCommands()).To(HaveLen(1))
		})
	})

	Context("Time Limit", func() {
		It("should keep re-sampling rejected samples until the time limit", func() {
			useInstanceTypes(1.95)
			makeNodes(2, nodePool, "3")
			src := &countingSource{Source: rand.NewPCG(1, 1)}
			s := newSampleAndDelete(src)

			Expect(computeUntilTimeLimit(s, src)).To(BeEmpty())
			// Each sample draws once per candidate.
			Expect(src.draws.Load() / 2).To(BeNumerically(">", 1))
			ExpectMetricCounterValue(disruption.ConsolidationTimeoutsTotal, 1, map[string]string{disruption.ConsolidationTypeLabel: "sample-and-delete"})
			// Like multi-node consolidation, it waits for the cluster to change before searching again.
			Expect(s.IsConsolidated()).To(BeTrue())
		})
		It("should not sample once the time limit has passed", func() {
			disruption.SampleAndDeleteTimeoutDuration = 0
			makeNodes(3, nodePool, "1")
			src := &countingSource{Source: rand.NewPCG(1, 1)}
			Expect(computeCommands(newSampleAndDelete(src))).To(BeEmpty())
			Expect(src.draws.Load()).To(BeZero())
			ExpectMetricCounterValue(disruption.ConsolidationTimeoutsTotal, 1, map[string]string{disruption.ConsolidationTypeLabel: "sample-and-delete"})
		})
	})

	Context("Budgets", func() {
		It("should only sample candidates that fit in the budget", func() {
			nodePool = newNodePool("2")
			makeNodes(5, nodePool, "1")
			for seed := range uint64(5) {
				cmds := computeCommands(newSampleAndDelete(rand.NewPCG(seed, seed)))
				Expect(cmds).To(HaveLen(1))
				Expect(cmds[0].Candidates).To(HaveLen(2))
			}
		})
		It("should keep each NodePool within its own budget", func() {
			nodePool = newNodePool("1")
			other := newNodePool("1")
			makeNodes(3, nodePool, "1")
			makeNodes(3, other, "1")
			cmds := computeCommands(newSampleAndDelete(rand.NewPCG(1, 1)))
			Expect(cmds).To(HaveLen(1))
			byPool := candidateNamesByPool(cmds[0])
			Expect(byPool[nodePool.Name]).To(HaveLen(1))
			Expect(byPool[other.Name]).To(HaveLen(1))
		})
		It("should do nothing when budgets leave fewer than two candidates", func() {
			nodePool = newNodePool("1")
			makeNodes(3, nodePool, "1")
			src := &countingSource{Source: rand.NewPCG(1, 1)}
			s := newSampleAndDelete(src)
			Expect(computeCommands(s)).To(BeEmpty())
			Expect(src.draws.Load()).To(BeZero())
			// A budget can free up without the cluster changing, so it tries again next time.
			Expect(s.IsConsolidated()).To(BeFalse())
		})
		It("should do nothing with fewer than two candidates", func() {
			makeNodes(1, nodePool, "1")
			s := newSampleAndDelete(rand.NewPCG(1, 1))
			Expect(computeCommands(s)).To(BeEmpty())
			Expect(s.IsConsolidated()).To(BeTrue())
		})
	})

	Context("Sampling", func() {
		It("should find the same command from the same seed", func() {
			makeNodes(6, nodePool, "1")
			first := computeCommands(newSampleAndDelete(rand.NewPCG(42, 7)))
			second := computeCommands(newSampleAndDelete(rand.NewPCG(42, 7)))
			Expect(first).To(HaveLen(1))
			Expect(second).To(HaveLen(1))
			Expect(candidateNamesByPool(second[0])).To(Equal(candidateNamesByPool(first[0])))
		})
		It("should make a node that needs repair more likely to be sampled", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
				FeatureGates:           test.FeatureGates{SampleAndDeleteConsolidation: new(true), NodeRepair: new(true)},
				SampleAndDeleteWeights: &options.SampleAndDeleteWeights{Repair: 1},
			}))
			_, nodes := makeNodes(2, nodePool, "1")
			unhealthy := ExpectExists(ctx, env.Client, nodes[0])
			// Repair measures the toleration from no earlier than the node's creation.
			if env.Clock.Now().Before(unhealthy.CreationTimestamp.Time) {
				env.Clock.SetTime(unhealthy.CreationTimestamp.Time)
			}
			unhealthy.Status.Conditions = append(unhealthy.Status.Conditions, corev1.NodeCondition{
				Type: "BadNode", Status: corev1.ConditionFalse, LastTransitionTime: metav1.Time{Time: env.Clock.Now()},
			})
			ExpectApplied(ctx, env.Client, unhealthy)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(unhealthy))
			// The fake cloud provider's repair policy tolerates BadNode for 30 minutes.
			env.Clock.Step(31 * time.Minute)

			s := newSampleAndDelete(rand.NewPCG(1, 1))
			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider,
				func(context.Context, *disruption.Candidate) bool { return true }, disruption.GracefulDisruptionClass, queue)
			Expect(err).ToNot(HaveOccurred())
			Expect(candidates).To(HaveLen(2))
			probabilityByName := func() map[string]float64 {
				probabilities := s.DeleteProbabilities(ctx, candidates)
				return lo.SliceToMap(lo.Range(len(candidates)), func(i int) (string, float64) { return candidates[i].Name(), probabilities[i] })
			}
			Expect(probabilityByName()).To(Equal(map[string]float64{nodes[0].Name: 1, nodes[1].Name: 0}))

			// Without node repair, the repair code doesn't act on the node, so the repair term doesn't either.
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
				FeatureGates:           test.FeatureGates{SampleAndDeleteConsolidation: new(true)},
				SampleAndDeleteWeights: &options.SampleAndDeleteWeights{Repair: 1},
			}))
			Expect(probabilityByName()).To(Equal(map[string]float64{nodes[0].Name: 0, nodes[1].Name: 0}))
		})
	})
})
