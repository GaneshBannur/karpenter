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

package node_test

import (
	"context"
	"testing"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	metricsnode "sigs.k8s.io/karpenter/pkg/controllers/metrics/node"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var cluster *state.Cluster
var nodeController *informer.NodeController
var nodeClaimController *informer.NodeClaimController
var metricsStateController *metricsnode.Controller
var cloudProvider *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodeMetrics")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))

	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	cluster = state.NewCluster(env.Clock, env.Client, cloudProvider)
	clusterCost := cost.NewClusterCost(ctx, cloudProvider, env.Client)
	nodeController = informer.NewNodeController(env.Client, cluster)
	nodeClaimController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
	metricsStateController = metricsnode.NewController(cluster)
})

var _ = AfterEach(func() {
	cluster.Reset()
})

var _ = AfterSuite(func() {
	ExpectCleanedUp(ctx, env.Client)
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Node Metrics", func() {
	var node *corev1.Node
	var resources corev1.ResourceList

	BeforeEach(func() {
		resources = corev1.ResourceList{
			corev1.ResourcePods:   resource.MustParse("100"),
			corev1.ResourceCPU:    resource.MustParse("5000"),
			corev1.ResourceMemory: resource.MustParse("32Gi"),
		}
		node = test.Node(test.NodeOptions{Allocatable: resources})
	})
	It("should update the allocatable metric", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectSingletonReconciled(ctx, metricsStateController)

		for k, v := range resources {
			// A plain node with no NodeClaim is not managed by Karpenter.
			metric, found := FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
				"node_name":               node.GetName(),
				metrics.ResourceTypeLabel: k.String(),
				"managed":                 "false",
			})
			Expect(found).To(BeTrue())
			Expect(metric.GetGauge().GetValue()).To(BeNumerically("~", v.AsApproximateFloat64()))
		}
	})
	It("should set the managed label on per-node metrics for Karpenter-managed nodes", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			Status: v1.NodeClaimStatus{
				ProviderID:  test.RandomProviderID(),
				Allocatable: resources,
			},
		})
		managedNode := test.Node(test.NodeOptions{
			ProviderID:  nodeClaim.Status.ProviderID,
			Allocatable: resources,
		})

		ExpectApplied(ctx, env.Client, managedNode, nodeClaim)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{managedNode}, []*v1.NodeClaim{nodeClaim})
		ExpectSingletonReconciled(ctx, metricsStateController)

		for k, v := range resources {
			metric, found := FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
				"node_name":               managedNode.GetName(),
				metrics.ResourceTypeLabel: k.String(),
				"managed":                 "true",
			})
			Expect(found).To(BeTrue())
			Expect(metric.GetGauge().GetValue()).To(BeNumerically("~", v.AsApproximateFloat64()))
		}
	})
	It("should update the node lifetime and cluster utilization metrics", func() {

		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectSingletonReconciled(ctx, metricsStateController)

		metric, found := FindMetricWithLabelValues("karpenter_nodes_current_lifetime_seconds", map[string]string{
			"node_name": node.GetName(),
			"managed":   "false",
		})
		Expect(found).To(BeTrue())
		Expect(metric.GetGauge().GetValue()).To(BeNumerically(">=", 0))

		for resourceName := range resources {
			metric, found := FindMetricWithLabelValues("karpenter_cluster_utilization_percent", map[string]string{
				metrics.ResourceTypeLabel: resourceName.String(),
			})
			Expect(found).To(BeTrue())
			Expect(metric.GetGauge().GetValue()).To(BeNumerically("==", 0))
		}
	})
	It("should remove the node metric gauge when the node is deleted", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectSingletonReconciled(ctx, metricsStateController)

		_, found := FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
			"node_name": node.GetName(),
		})
		Expect(found).To(BeTrue())

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectSingletonReconciled(ctx, metricsStateController)

		_, found = FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
			"node_name": node.GetName(),
		})
		Expect(found).To(BeFalse())
	})
	Context("do-not-repair nodes metric", func() {
		doNotRepair := func() map[string]string { return map[string]string{v1.DoNotRepairAnnotationKey: "true"} }

		newManagedNode := func(nodePoolName string, annotations map[string]string) (*v1.NodeClaim, *corev1.Node) {
			return test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{v1.NodePoolLabelKey: nodePoolName},
					Annotations: annotations,
				},
				Status: v1.NodeClaimStatus{Allocatable: resources},
			})
		}
		// Registering is what makes cluster state read the annotation from the Node instead of the NodeClaim.
		syncManagedNode := func(nodeClaim *v1.NodeClaim, managedNode *corev1.Node) {
			GinkgoHelper()
			ExpectApplied(ctx, env.Client, managedNode, nodeClaim)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{managedNode}, []*v1.NodeClaim{nodeClaim})
		}
		expectDoNotRepairNodes := func(nodePoolName string, count float64) {
			GinkgoHelper()
			ExpectMetricGaugeValue(metricsnode.NodePoolDoNotRepairNodes, count, map[string]string{metrics.NodePoolLabel: nodePoolName})
		}
		expectNoSeries := func(nodePoolName string) {
			GinkgoHelper()
			metricName := ExpectMetricName(metricsnode.NodePoolDoNotRepairNodes.(*opmetrics.PrometheusGauge))
			_, found := FindMetricWithLabelValues(metricName, map[string]string{metrics.NodePoolLabel: nodePoolName})
			Expect(found).To(BeFalse())
		}

		It("should count each NodePool's nodes whose annotation is exactly true", func() {
			poolA, poolB := test.RandomName(), test.RandomName()
			syncManagedNode(newManagedNode(poolA, doNotRepair()))
			syncManagedNode(newManagedNode(poolA, map[string]string{v1.DoNotRepairAnnotationKey: "false"}))
			// Node repair compares against "true", so "True" doesn't count.
			syncManagedNode(newManagedNode(poolA, map[string]string{v1.DoNotRepairAnnotationKey: "True"}))
			syncManagedNode(newManagedNode(poolA, nil))
			syncManagedNode(newManagedNode(poolB, nil))
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectDoNotRepairNodes(poolA, 1)
			expectDoNotRepairNodes(poolB, 0)
		})
		It("should drop back to 0 when the annotation is removed", func() {
			nodePoolName := test.RandomName()
			nodeClaim, managedNode := newManagedNode(nodePoolName, doNotRepair())
			syncManagedNode(nodeClaim, managedNode)
			ExpectSingletonReconciled(ctx, metricsStateController)
			expectDoNotRepairNodes(nodePoolName, 1)

			// Read the Node back first, so applying it keeps the labels that registered it.
			registeredNode := ExpectExists(ctx, env.Client, managedNode)
			registeredNode.Annotations = nil
			ExpectApplied(ctx, env.Client, registeredNode)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(registeredNode))
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectDoNotRepairNodes(nodePoolName, 0)
		})
		It("should ignore the NodeClaim's annotation once the Node registers", func() {
			claimOnlyPool, nodeOnlyPool := test.RandomName(), test.RandomName()
			claimOnlyClaim, claimOnlyNode := newManagedNode(claimOnlyPool, nil)
			claimOnlyClaim.Annotations = doNotRepair()
			nodeOnlyClaim, nodeOnlyNode := newManagedNode(nodeOnlyPool, nil)
			nodeOnlyNode.Annotations = doNotRepair()
			syncManagedNode(claimOnlyClaim, claimOnlyNode)
			syncManagedNode(nodeOnlyClaim, nodeOnlyNode)
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectDoNotRepairNodes(claimOnlyPool, 0)
			expectDoNotRepairNodes(nodeOnlyPool, 1)
		})
		It("should read the NodeClaim's annotation while the Node is unregistered", func() {
			nodePoolName := test.RandomName()
			nodeClaim, unregisteredNode := newManagedNode(nodePoolName, doNotRepair())
			unregisteredNode.Annotations = nil
			// Cluster state only tracks a managed Node once it has an instance type label.
			unregisteredNode.Labels[corev1.LabelInstanceTypeStable] = "default-instance-type"
			ExpectApplied(ctx, env.Client, unregisteredNode, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(unregisteredNode))
			Expect(ExpectStateNodeExists(cluster, unregisteredNode).Registered()).To(BeFalse())
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectDoNotRepairNodes(nodePoolName, 1)
		})
		DescribeTable("should not count nodes being deleted",
			func(markDeleting func(*v1.NodeClaim, *corev1.Node)) {
				nodePoolName := test.RandomName()
				deletingClaim, deletingNode := newManagedNode(nodePoolName, doNotRepair())
				syncManagedNode(deletingClaim, deletingNode)
				markDeleting(deletingClaim, deletingNode)
				ExpectSingletonReconciled(ctx, metricsStateController)

				// The NodePool still has a node, so it reports 0 rather than losing its series.
				expectDoNotRepairNodes(nodePoolName, 0)
			},
			Entry("marked for deletion in cluster state", func(nodeClaim *v1.NodeClaim, _ *corev1.Node) {
				cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
			}),
			Entry("the NodeClaim is terminating", func(nodeClaim *v1.NodeClaim, _ *corev1.Node) {
				ExpectDeletionTimestampSet(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			}),
		)
		It("should not count nodes that Karpenter doesn't manage", func() {
			nodePoolName := test.RandomName()
			// A Node with the NodePool label but no NodeClaim. Cluster state tracks it once it has an instance type.
			unmanagedNode := test.Node(test.NodeOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{v1.NodePoolLabelKey: nodePoolName, corev1.LabelInstanceTypeStable: "default-instance-type"},
					Annotations: doNotRepair(),
				},
				ProviderID:  test.RandomProviderID(),
				Allocatable: resources,
			})
			ExpectApplied(ctx, env.Client, unmanagedNode)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(unmanagedNode))
			Expect(ExpectStateNodeExists(cluster, unmanagedNode).Managed()).To(BeFalse())
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectNoSeries(nodePoolName)
		})
		It("should not report a NodeClaim that has no NodePool", func() {
			nodeClaim, managedNode := test.NodeClaimAndNode(v1.NodeClaim{
				Status: v1.NodeClaimStatus{Allocatable: resources},
			})
			nodeClaim.Annotations = doNotRepair()
			syncManagedNode(nodeClaim, managedNode)
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectNoSeries("")
		})
		It("should remove a NodePool's series once it has no nodes", func() {
			nodePoolName := test.RandomName()
			nodeClaim, managedNode := newManagedNode(nodePoolName, doNotRepair())
			syncManagedNode(nodeClaim, managedNode)
			ExpectSingletonReconciled(ctx, metricsStateController)
			expectDoNotRepairNodes(nodePoolName, 1)

			ExpectDeleted(ctx, env.Client, managedNode, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(managedNode))
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectSingletonReconciled(ctx, metricsStateController)

			expectNoSeries(nodePoolName)
		})
	})
})
