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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestStaticBlockedReason(t *testing.T) {
	const reason = "static NodePool is at its node limit and cannot stage a replacement"
	const notReadyReason = reason + ", and the node cannot be terminated first because the NodePool is not Ready"
	const deletingReason = reason + ", and the node cannot be terminated first because the NodePool is being deleted"

	ready := test.StaticNodePool()
	notReady := test.StaticNodePool()
	notReady.StatusConditions().SetFalse(v1.ConditionTypeValidationSucceeded, "NotReady", "NotReady")
	deleting := test.StaticNodePool()
	deleting.DeletionTimestamp = new(metav1.Now())
	deletingNotReady := notReady.DeepCopy()
	deletingNotReady.DeletionTimestamp = new(metav1.Now())

	tests := []struct {
		name               string
		nodePool           *v1.NodePool
		wantTerminateFirst bool
		expectedReason     string
		expectedBlocked    bool
	}{
		{name: "not terminating first, Ready NodePool", nodePool: ready, expectedReason: reason, expectedBlocked: true},
		{name: "not terminating first, NotReady NodePool", nodePool: notReady, expectedReason: reason, expectedBlocked: true},
		{name: "not terminating first, NodePool being deleted", nodePool: deleting, expectedReason: reason, expectedBlocked: true},
		{name: "terminating first, Ready NodePool", nodePool: ready, wantTerminateFirst: true},
		{name: "terminating first, NotReady NodePool", nodePool: notReady, wantTerminateFirst: true, expectedReason: notReadyReason, expectedBlocked: true},
		{name: "terminating first, NodePool being deleted", nodePool: deleting, wantTerminateFirst: true, expectedReason: deletingReason, expectedBlocked: true},
		{name: "terminating first, NotReady NodePool being deleted", nodePool: deletingNotReady, wantTerminateFirst: true, expectedReason: deletingReason, expectedBlocked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReason, gotBlocked := staticBlockedReason(tt.nodePool, tt.wantTerminateFirst, reason)
			if gotReason != tt.expectedReason || gotBlocked != tt.expectedBlocked {
				t.Errorf("staticBlockedReason() = (%q, %v), want (%q, %v)", gotReason, gotBlocked, tt.expectedReason, tt.expectedBlocked)
			}
		})
	}
}
