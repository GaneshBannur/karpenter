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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	voluntaryDisruptionSubsystem = "voluntary_disruption"
	decisionLabel                = "decision"
	ConsolidationTypeLabel       = "consolidation_type"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"
	terminateFirstReasonLabel    = "terminate_first_reason"
)

var (
	MultiNodeConsolidationType = opmetrics.Value{
		Name: "multi",
		Help: "Consolidation that considers removing multiple nodes at once.",
	}
	SingleNodeConsolidationType = opmetrics.Value{
		Name: "single",
		Help: "Consolidation that considers removing a single node.",
	}
	EmptyConsolidationType = opmetrics.Value{
		Name: "empty",
		Help: "Consolidation that removes empty nodes.",
	}
)

var (
	ConsolidationType = opmetrics.Label{
		Name:   ConsolidationTypeLabel,
		Help:   "The consolidation algorithm that produced the decision.",
		Values: []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType, EmptyConsolidationType},
	}
	// DecisionDim is the `decision` dimension for the voluntary-disruption decision
	// counters, whose value is the command's action.
	DecisionDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "The disruption decision taken for the candidate(s).",
		Values: []opmetrics.Value{
			{
				Name: string(NoOpDecision),
				Help: "No disruption action was taken.",
			},
			{
				Name: string(ReplaceDecision),
				Help: "The candidate(s) were replaced with more efficient capacity.",
			},
			{
				Name: string(DeleteDecision),
				Help: "The candidate(s) were deleted without replacement.",
			},
			{
				Name: string(TerminateFirstDecision),
				Help: "The candidate(s) were deleted without staging a replacement first; reactive provisioning refills afterward.",
			},
			{
				Name: string(RebootDecision),
				Help: "The candidate(s) were rebooted in place.",
			},
		},
	}
	// ApprovalDim is the `decision` dimension for the balanced-consolidation move
	// metrics, which score each candidate move and record whether it was approved or
	// rejected — a disjoint value set from DecisionDim, so it is a separate Label.
	ApprovalDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "Whether a scored balanced-consolidation move was approved or rejected.",
		Values: []opmetrics.Value{
			{
				Name: string(ApprovedDecision),
				Help: "The move's cost savings justified the pod disruption; it was approved.",
			},
			{
				Name: string(RejectedDecision),
				Help: "The move's cost savings did not justify the pod disruption; it was rejected.",
			},
		},
	}
	Policy = opmetrics.Label{
		Name: policyLabel,
		Help: "The NodePool consolidation policy in effect for the move.",
	}
	TerminateFirstReasonDim = opmetrics.Label{
		Name: terminateFirstReasonLabel,
		Help: "Why the terminate-first command couldn't stage its replacement before terminating the node.",
		Values: []opmetrics.Value{
			{
				Name: string(TerminateFirstNoReservedCapacity),
				Help: "The replacement has no reserved capacity to launch into except the slot the node holds in a full capacity reservation.",
			},
			{
				Name: string(TerminateFirstStaticAtLimit),
				Help: "The node's static NodePool is at its node limit, so the replacement can't launch alongside it.",
			},
		},
	}
)

func init() {
	// Initialize the consolidation_type series that can time out to 0. Only the
	// multi- and single-node algorithms run a bounded search that can hit a timeout;
	// empty-node consolidation does not, so it is not pre-initialized here.
	for _, ct := range []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType} {
		ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: ct.Name})
	}
}

var (
	EvaluationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_duration_seconds",
			Help:      "Duration of the disruption decision evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.GA,
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{metrics.NodePool, DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	TerminateFirstDecisionsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "terminate_first_decisions_total",
			Help:      "Number of terminate-first disruption decisions performed. Labeled by nodepool name, reason, and why the replacement couldn't be staged first.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason, TerminateFirstReasonDim},
		opmetrics.Alpha,
	)
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]opmetrics.Label{metrics.DisruptionReason},
		opmetrics.Beta,
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Beta,
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Alpha,
	)
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.GA,
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.Alpha,
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	// NodeClaimsUnhealthyDisruptedTotal preserves the per-condition/per-image breakdown the retired node.health
	// controller emitted, which the reason-labeled karpenter_nodeclaims_disrupted_total loses. Labeled by the repair
	// condition, the owning NodePool, the capacity type, and the image ID.
	NodeClaimsUnhealthyDisruptedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeClaimSubsystem,
			Name:      "unhealthy_disrupted_total",
			Help:      "Number of unhealthy nodeclaims disrupted in total by node repair. Labeled by the condition the node was disrupted on, the owning nodepool, the capacity type, the image ID, and the termination mode.",
		},
		[]opmetrics.Label{RepairCondition, metrics.NodePool, metrics.CapacityType, ImageID, metrics.TerminationMode},
		opmetrics.Alpha,
	)
	// NodeClaimsUnhealthyBlocked tells a stalled repair apart from one with nothing to do. Unlike EligibleNodes, it says
	// why, per NodePool, and also counts nodes vetoed by do-not-repair or blocking pods.
	NodeClaimsUnhealthyBlocked = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeClaimSubsystem,
			Name:      "unhealthy_blocked",
			Help:      "Number of unhealthy nodeclaims past their repair toleration that node repair is holding back. Labeled by the owning nodepool and why repair is holding them back. Nodes that repair skips before checking them, such as nodes nominated for pending pods, aren't counted. Updated after each repair pass; a pass that fails before checking every candidate keeps the previous value.",
		},
		[]opmetrics.Label{metrics.NodePool, RepairBlockedReason},
		opmetrics.Alpha,
	)
)

const (
	conditionLabel           = "condition"
	imageIDLabel             = "image_id"
	repairBlockedReasonLabel = "blocked_reason"
)

var (
	RepairCondition = opmetrics.Label{
		Name: conditionLabel,
		Help: "The node status condition type that triggered node repair disruption.",
	}
	ImageID = opmetrics.Label{
		Name: imageIDLabel,
		Help: "The image ID of the node that was disrupted.",
	}
)

var (
	RepairBlockedDoNotRepair = opmetrics.Value{
		Name: "do-not-repair",
		Help: "The node carries the karpenter.sh/do-not-repair: \"true\" annotation.",
	}
	RepairBlockedBlockingPods = opmetrics.Value{
		Name: "blocking-pods",
		Help: "The node has pods that block eviction, such as do-not-disrupt pods or pods whose PodDisruptionBudget allows no disruptions right now, and neither the repair policy nor the nodeclaim sets a termination grace period.",
	}
	RepairBlockedBreaker = opmetrics.Value{
		Name: "breaker",
		Help: "More than " + repairUnhealthyThreshold + " of the nodepool's nodes are unhealthy, so repair is paused for the nodepool.",
	}
	RepairBlockedBudget = opmetrics.Value{
		Name: "budget",
		Help: "The nodepool's disruption budget allows no more Unhealthy disruptions right now: it's set to zero, disruptions in progress use it all, or one of its budgets is misconfigured.",
	}
	RepairBlockedNoReplacement = opmetrics.Value{
		Name: "no-replacement",
		Help: "Repair can't stage a replacement: some pods can't be scheduled elsewhere, or a static nodepool is at its node limit or its capacity reservations are full. Can undercount, since each pass stops checking nodes once repair acts on one.",
	}
	RepairBlockedWaitingOnInitialization = opmetrics.Value{
		Name: "waiting-on-initialization",
		Help: "The node's pods would only fit on a node that hasn't finished initializing, so repair waits for it. This is usually brief; a long wait means that node is stuck starting. Can undercount, like no-replacement.",
	}
	RepairBlockedReason = opmetrics.Label{
		Name: repairBlockedReasonLabel,
		Help: "Why node repair is holding back an unhealthy node. Each node is counted once, under the first reason that applies, in the order listed.",
		// Listed in the order repair checks them, which the Help relies on.
		Values: []opmetrics.Value{RepairBlockedDoNotRepair, RepairBlockedBlockingPods, RepairBlockedBreaker, RepairBlockedBudget, RepairBlockedNoReplacement,
			RepairBlockedWaitingOnInitialization},
	}
)
