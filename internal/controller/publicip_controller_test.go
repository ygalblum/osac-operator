/*
Copyright 2026.

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

package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	runtimecluster "sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mc "sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	osacv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/internal/provisioning"
)

// mockMulticlusterManager is a minimal implementation for testing address population.
// Embeds the full Manager interface; only GetCluster is overridden.
type mockMulticlusterManager struct {
	mcmanager.Manager
	targetClient client.Client
}

func (m *mockMulticlusterManager) GetCluster(_ context.Context, _ mc.ClusterName) (runtimecluster.Cluster, error) {
	return &mockCluster{client: m.targetClient}, nil
}

// mockCluster satisfies cluster.Cluster; only GetClient is overridden.
type mockCluster struct {
	runtimecluster.Cluster
	client client.Client
}

func (c *mockCluster) GetClient() client.Client {
	return c.client
}

// Tests for the PublicIP provisioning controller.
//
// Key concepts for understanding these tests:
//
//   - A PublicIP belongs to a parent PublicIPPool. The relationship uses fulfillment-service
//     UUIDs, not K8s object names: publicIP.spec.pool contains a UUID, and the parent
//     PublicIPPool CR carries that UUID in its osac.openshift.io/publicippool-uuid label.
//     The controller resolves the parent by listing pools with a matching label.
//
//   - The reconcile loop requires multiple passes because each pass does one thing and
//     returns: first pass adds the finalizer (metadata write), second pass processes the
//     spec and triggers provisioning, third pass polls the provisioning job status.
//
//   - Deletion tests call handleDelete directly because the fake client does not support
//     setting DeletionTimestamp via Update. We set it in memory and invoke the handler.
//
//   - The mock provisioning provider (defined in computeinstance_provisioning_test.go)
//     simulates AAP job triggers and status polls. By default it returns success; tests
//     override specific funcs to simulate failures or running states.
var _ = Describe("PublicIPReconciler", func() {
	var (
		reconciler   *PublicIPReconciler
		mockProvider *mockProvisioningProvider
		fakeClient   client.Client
		testCtx      context.Context
		publicIP     *osacv1alpha1.PublicIP
		parentPool   *osacv1alpha1.PublicIPPool
		testScheme   *runtime.Scheme
	)

	const (
		testNamespace            = "test-namespace"
		testPoolUUID             = "pool-uuid-123"
		testConfigVersion        = "version-1-abc123"
		testConfigVersionUpdated = "version-2-def456"
		testComputeInstance      = "some-ci"
	)

	BeforeEach(func() {
		testCtx = context.TODO()
		testScheme = runtime.NewScheme()
		Expect(osacv1alpha1.AddToScheme(testScheme)).To(Succeed())
		Expect(scheme.AddToScheme(testScheme)).To(Succeed())

		// The parent pool has a K8s name ("pool-k8s-name") that differs from the UUID
		// ("pool-uuid-123") to verify the controller uses label-based lookup, not name-based.
		parentPool = &osacv1alpha1.PublicIPPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pool-k8s-name",
				Namespace: testNamespace,
				Labels: map[string]string{
					osacPublicIPPoolIDLabel: testPoolUUID,
				},
			},
			Spec: osacv1alpha1.PublicIPPoolSpec{
				CIDRs:                  []string{"192.168.1.0/24"},
				IPFamily:               "IPv4",
				ImplementationStrategy: "metallb-l2",
			},
		}

		// The PublicIP references the parent pool by UUID, not by K8s name.
		publicIP = &osacv1alpha1.PublicIP{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-publicip",
				Namespace: testNamespace,
			},
			Spec: osacv1alpha1.PublicIPSpec{
				Pool: testPoolUUID,
			},
		}

		// ComputeInstance referenced by attach/detach tests via testComputeInstance UUID.
		testCI := &osacv1alpha1.ComputeInstance{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-ci-k8s",
				Namespace: testNamespace,
				Labels: map[string]string{
					osacComputeInstanceIDLabel: testComputeInstance,
				},
			},
			Status: osacv1alpha1.ComputeInstanceStatus{
				VirtualMachineReference: &osacv1alpha1.VirtualMachineReferenceType{
					Namespace:                  testNamespace,
					KubeVirtVirtualMachineName: "test-vm",
				},
			},
		}

		fakeClient = fake.NewClientBuilder().
			WithScheme(testScheme).
			WithObjects(publicIP, parentPool, testCI).
			WithStatusSubresource(&osacv1alpha1.PublicIP{}, &osacv1alpha1.ComputeInstance{}).
			Build()

		// WithStatusSubresource strips status from WithObjects, so persist it separately.
		Expect(fakeClient.Status().Update(testCtx, testCI)).To(Succeed())

		mockProvider = &mockProvisioningProvider{name: "mock-aap"}

		// Default mock mgr provides an empty workload-cluster client so the
		// address-population guard in handleUpdate does not nil-panic. Tests
		// that need a Service on the workload cluster override reconciler.mgr.
		emptyTargetClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

		reconciler = &PublicIPReconciler{
			Client:                   fakeClient,
			APIReader:                fakeClient,
			Scheme:                   testScheme,
			mgr:                      &mockMulticlusterManager{targetClient: emptyTargetClient},
			NetworkingNamespace:      testNamespace,
			ComputeInstanceNamespace: testNamespace,
			ProvisioningProvider:     mockProvider,
			StatusPollInterval:       1 * time.Second,
			MaxJobHistory:            10,
		}
	})

	Context("Reconcile", func() {
		It("should add finalizer on first reconcile", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}
			result, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Finalizers).To(ContainElement(osacPublicIPFinalizer))
		})

		It("should set phase to Progressing initially", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Keep the job in Running state so the phase stays Progressing
			// (a Succeeded job would transition to Ready)
			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateRunning,
					Message: "Job running",
				}, nil
			}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: resolves parent pool, triggers provisioning, sets Progressing
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseProgressing))
		})

		It("should set implementation-strategy annotation from parent pool", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: resolves parent pool and copies its implementation strategy
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Annotations[osacImplementationStrategyAnnotation]).To(Equal("metallb-l2"))
			Expect(updated.Annotations[osacPublicIPPoolNameAnnotation]).To(Equal("pool-k8s-name"))
		})

		It("should requeue when parent PublicIPPool is not found", func() {
			// This PublicIP references a pool UUID that doesn't exist as a label
			// on any PublicIPPool CR. The controller should requeue and wait for
			// the pool to appear (it may not have been reconciled to K8s yet).
			orphanIP := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "orphan-publicip",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool: "nonexistent-pool-uuid",
				},
			}
			Expect(fakeClient.Create(testCtx, orphanIP)).To(Succeed())

			key := types.NamespacedName{Name: orphanIP.Name, Namespace: orphanIP.Namespace}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: parent pool not found, requeues after precondition interval
			result, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(defaultPreconditionRequeueInterval))
		})

		It("should use default implementation strategy when pool has none", func() {
			// A pool with no ImplementationStrategy in its spec should fall back
			// to defaultPublicIPPoolImplementationStrategy ("metallb-l2").
			poolNoStrategy := &osacv1alpha1.PublicIPPool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pool-no-strategy",
					Namespace: testNamespace,
					Labels: map[string]string{
						osacPublicIPPoolIDLabel: "pool-no-strategy-uuid",
					},
				},
				Spec: osacv1alpha1.PublicIPPoolSpec{
					CIDRs:    []string{"10.0.0.0/24"},
					IPFamily: "IPv4",
				},
			}
			Expect(fakeClient.Create(testCtx, poolNoStrategy)).To(Succeed())

			ipNoStrategy := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-no-strategy",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool: "pool-no-strategy-uuid",
				},
			}
			Expect(fakeClient.Create(testCtx, ipNoStrategy)).To(Succeed())

			key := types.NamespacedName{Name: ipNoStrategy.Name, Namespace: ipNoStrategy.Namespace}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: inherits default strategy since pool has none
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Annotations[osacImplementationStrategyAnnotation]).To(Equal(defaultPublicIPPoolImplementationStrategy))
			Expect(updated.Annotations[osacPublicIPPoolNameAnnotation]).To(Equal("pool-no-strategy"))
		})

		It("should set ConfigurationApplied condition to True", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Pass 1: finalizer, Pass 2: process spec and set condition
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())

			condition := osacv1alpha1.GetPublicIPStatusCondition(
				updated, osacv1alpha1.PublicIPConditionConfigurationApplied,
			)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal("ConfigurationApplied"))
		})

		It("should set phase to Ready on successful provision", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "job-success",
					InitialState: osacv1alpha1.JobStatePending,
					Message:      "Job triggered",
				}, nil
			}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateSucceeded,
					Message: "Provisioning completed",
				}, nil
			}

			// Pass 1: finalizer, Pass 2: trigger AAP job, Pass 3: poll job -> Succeeded -> Ready
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseReady))
		})

		It("should set phase to Failed on provision failure", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "job-fail",
					InitialState: osacv1alpha1.JobStatePending,
					Message:      "Job triggered",
				}, nil
			}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:        jobID,
					State:        osacv1alpha1.JobStateFailed,
					Message:      "Provisioning failed",
					ErrorDetails: "MetalLB unreachable",
				}, nil
			}

			// Pass 1: finalizer, Pass 2: trigger, Pass 3: poll -> Failed
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseFailed))
		})

		// Deletion tests call handleDelete directly because the fake client does not
		// support setting DeletionTimestamp via Update. We add the finalizer via a
		// normal Reconcile, then set DeletionTimestamp in memory and call handleDelete.

		It("should trigger deprovision on delete", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			deprovisionCalled := false
			mockProvider.triggerDeprovisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.DeprovisionResult, error) {
				deprovisionCalled = true
				return &provisioning.DeprovisionResult{
					Action:                 provisioning.DeprovisionTriggered,
					JobID:                  "deprovision-job-123",
					BlockDeletionOnFailure: true,
				}, nil
			}

			// Add finalizer via normal reconcile
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Simulate K8s delete by setting DeletionTimestamp in memory
			toDelete := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, toDelete)).To(Succeed())
			now := metav1.Now()
			toDelete.DeletionTimestamp = &now

			_, err = reconciler.handleDelete(testCtx, toDelete)
			Expect(err).NotTo(HaveOccurred())

			Expect(deprovisionCalled).To(BeTrue())
			Expect(toDelete.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseDeleting))

			latestJob := provisioning.FindLatestJobByType(toDelete.Status.Jobs, osacv1alpha1.JobTypeDeprovision)
			Expect(latestJob).NotTo(BeNil())
			Expect(latestJob.JobID).To(Equal("deprovision-job-123"))
		})

		It("should remove finalizer after successful deprovision", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			mockProvider.triggerDeprovisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.DeprovisionResult, error) {
				return &provisioning.DeprovisionResult{
					Action:                 provisioning.DeprovisionTriggered,
					JobID:                  "deprovision-success",
					BlockDeletionOnFailure: true,
				}, nil
			}

			mockProvider.getDeprovisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateSucceeded,
					Message: "Deprovision completed",
				}, nil
			}

			// Add finalizer via normal reconcile
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			toDelete := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, toDelete)).To(Succeed())
			now := metav1.Now()
			toDelete.DeletionTimestamp = &now

			// First call triggers the deprovision job
			_, err = reconciler.handleDelete(testCtx, toDelete)
			Expect(err).NotTo(HaveOccurred())

			// Second call polls status (Succeeded) and removes the finalizer
			_, _ = reconciler.handleDelete(testCtx, toDelete)

			Expect(toDelete.Finalizers).NotTo(ContainElement(osacPublicIPFinalizer))
		})

		It("should still handle delete for unmanaged PublicIP with finalizer", func() {
			// Edge case: a PublicIP was managed (has finalizer), then an admin marked
			// it unmanaged. The management-state guard in Reconcile skips processing
			// for non-deleted resources, but deletion must still proceed to clean up
			// the AAP-provisioned resources and remove the finalizer.
			managedThenUnmanaged := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "managed-then-unmanaged",
					Namespace: testNamespace,
					Annotations: map[string]string{
						osacManagementStateAnnotation: ManagementStateUnmanaged,
					},
					Finalizers: []string{osacPublicIPFinalizer},
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool: testPoolUUID,
				},
			}
			Expect(fakeClient.Create(testCtx, managedThenUnmanaged)).To(Succeed())

			key := types.NamespacedName{Name: managedThenUnmanaged.Name, Namespace: managedThenUnmanaged.Namespace}

			deprovisionCalled := false
			mockProvider.triggerDeprovisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.DeprovisionResult, error) {
				deprovisionCalled = true
				return &provisioning.DeprovisionResult{
					Action: provisioning.DeprovisionSkipped,
				}, nil
			}

			fetched := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, fetched)).To(Succeed())
			now := metav1.Now()
			fetched.DeletionTimestamp = &now

			// Verify the guard logic: DeletionTimestamp is set, so the unmanaged
			// annotation is ignored and the delete branch runs.
			Expect(fetched.ObjectMeta.DeletionTimestamp.IsZero()).To(BeFalse())

			_, _ = reconciler.handleDelete(testCtx, fetched)

			Expect(deprovisionCalled).To(BeTrue())
			Expect(fetched.Finalizers).NotTo(ContainElement(osacPublicIPFinalizer))
		})

		It("should set state to Pending on initial provisioning", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateRunning,
					Message: "Job running",
				}, nil
			}

			// Pass 1: finalizer, Pass 2: trigger provisioning -> Progressing + Pending
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseProgressing))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStatePending))
		})

		It("should set state to Allocated on successful initial provision with no ComputeInstance", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "job-allocated",
					InitialState: osacv1alpha1.JobStatePending,
					Message:      "Job triggered",
				}, nil
			}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateSucceeded,
					Message: "Provisioning completed",
				}, nil
			}

			// Pass 1: finalizer, Pass 2: trigger, Pass 3: poll -> Ready + Allocated
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseReady))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStateAllocated))
		})

		It("should set state to Attaching when ComputeInstance is set on allocated IP", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Start with an Allocated PublicIP (Ready phase, Allocated state)
			publicIP.Status.Phase = osacv1alpha1.PublicIPPhaseReady
			publicIP.Status.State = osacv1alpha1.PublicIPStateAllocated
			publicIP.Status.DesiredConfigVersion = testConfigVersion
			publicIP.Status.Jobs = []osacv1alpha1.JobStatus{
				{
					JobID:         "initial-provision",
					Type:          osacv1alpha1.JobTypeProvision,
					State:         osacv1alpha1.JobStateSucceeded,
					ConfigVersion: testConfigVersion,
					Timestamp:     metav1.NewTime(time.Now().UTC()),
				},
			}
			Expect(fakeClient.Status().Update(testCtx, publicIP)).To(Succeed())

			// Update spec to set ComputeInstance, which changes config version
			publicIP.Spec.ComputeInstance = testComputeInstance
			Expect(fakeClient.Update(testCtx, publicIP)).To(Succeed())

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateRunning,
					Message: "Job running",
				}, nil
			}

			// Reconcile should detect attach transition -> Progressing + Attaching
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseProgressing))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStateAttaching))
		})

		It("should set state to Attached when attach provisioning succeeds", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Start in Attaching state: persist spec first, then status.
			publicIP.Spec.ComputeInstance = testComputeInstance
			Expect(fakeClient.Update(testCtx, publicIP)).To(Succeed())

			publicIP.Status.Phase = osacv1alpha1.PublicIPPhaseProgressing
			publicIP.Status.State = osacv1alpha1.PublicIPStateAttaching
			Expect(fakeClient.Status().Update(testCtx, publicIP)).To(Succeed())

			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "attach-job",
					InitialState: osacv1alpha1.JobStatePending,
					Message:      "Attach job triggered",
				}, nil
			}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateSucceeded,
					Message: "Attach completed",
				}, nil
			}

			// Trigger attach job, then poll -> Ready + Attached
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseReady))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStateAttached))
		})

		It("should set state to Releasing when ComputeInstance is cleared on attached IP", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Start with an Attached PublicIP: set spec first, then status.
			// With WithStatusSubresource, Update() strips status from the
			// in-memory object, so Status().Update() must come last.
			publicIP.Spec.ComputeInstance = testComputeInstance
			Expect(fakeClient.Update(testCtx, publicIP)).To(Succeed())

			publicIP.Status.Phase = osacv1alpha1.PublicIPPhaseReady
			publicIP.Status.State = osacv1alpha1.PublicIPStateAttached
			publicIP.Status.DesiredConfigVersion = testConfigVersionUpdated
			publicIP.Status.Jobs = []osacv1alpha1.JobStatus{
				{
					JobID:         "attach-job",
					Type:          osacv1alpha1.JobTypeProvision,
					State:         osacv1alpha1.JobStateSucceeded,
					ConfigVersion: testConfigVersionUpdated,
					Timestamp:     metav1.NewTime(time.Now().UTC()),
				},
			}
			Expect(fakeClient.Status().Update(testCtx, publicIP)).To(Succeed())

			// Clear ComputeInstance (re-read first to get current resourceVersion)
			Expect(fakeClient.Get(testCtx, key, publicIP)).To(Succeed())
			publicIP.Spec.ComputeInstance = ""
			Expect(fakeClient.Update(testCtx, publicIP)).To(Succeed())

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateRunning,
					Message: "Job running",
				}, nil
			}

			// Reconcile should detect detach transition -> Progressing + Releasing
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseProgressing))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStateReleasing))
		})

		It("should set state to Allocated when detach provisioning succeeds", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Start in Releasing state: persist spec first, then status.
			publicIP.Spec.ComputeInstance = ""
			Expect(fakeClient.Update(testCtx, publicIP)).To(Succeed())

			publicIP.Status.Phase = osacv1alpha1.PublicIPPhaseProgressing
			publicIP.Status.State = osacv1alpha1.PublicIPStateReleasing
			Expect(fakeClient.Status().Update(testCtx, publicIP)).To(Succeed())

			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "detach-job",
					InitialState: osacv1alpha1.JobStatePending,
					Message:      "Detach job triggered",
				}, nil
			}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateSucceeded,
					Message: "Detach completed",
				}, nil
			}

			// Trigger detach job, then poll -> Ready + Allocated
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseReady))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStateAllocated))
		})

		It("should set state to Failed on provisioning failure", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "job-fail",
					InitialState: osacv1alpha1.JobStatePending,
					Message:      "Job triggered",
				}, nil
			}

			mockProvider.getProvisionStatusFunc = func(
				ctx context.Context, resource client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:        jobID,
					State:        osacv1alpha1.JobStateFailed,
					Message:      "Provisioning failed",
					ErrorDetails: "MetalLB unreachable",
				}, nil
			}

			// Pass 1: finalizer, Pass 2: trigger, Pass 3: poll -> Failed state + phase
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseFailed))
			Expect(updated.Status.State).To(Equal(osacv1alpha1.PublicIPStateFailed))
		})

		It("should not change state on deletion", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// Start with Allocated state: persist metadata first, then status.
			publicIP.Finalizers = []string{osacPublicIPFinalizer}
			Expect(fakeClient.Update(testCtx, publicIP)).To(Succeed())

			publicIP.Status.Phase = osacv1alpha1.PublicIPPhaseReady
			publicIP.Status.State = osacv1alpha1.PublicIPStateAllocated
			Expect(fakeClient.Status().Update(testCtx, publicIP)).To(Succeed())

			// Return a running deprovision job so handleDelete requeues before
			// reaching the finalizer-removal Update (which the envtest server
			// rejects when DeletionTimestamp is set in-memory).
			mockProvider.triggerDeprovisionFunc = func(
				_ context.Context, _ client.Object,
			) (*provisioning.DeprovisionResult, error) {
				return &provisioning.DeprovisionResult{
					Action: provisioning.DeprovisionTriggered,
					JobID:  "deprov-job",
				}, nil
			}

			toDelete := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, toDelete)).To(Succeed())
			now := metav1.Now()
			toDelete.DeletionTimestamp = &now

			_, err := reconciler.handleDelete(testCtx, toDelete)
			Expect(err).NotTo(HaveOccurred())

			// Phase transitions to Deleting but State remains unchanged
			Expect(toDelete.Status.Phase).To(Equal(osacv1alpha1.PublicIPPhaseDeleting))
			Expect(toDelete.Status.State).To(Equal(osacv1alpha1.PublicIPStateAllocated))
		})

		It("should ignore PublicIP with management-state unmanaged annotation", func() {
			// When a PublicIP has the unmanaged annotation and is NOT being deleted,
			// the controller should skip it entirely: no finalizer, no phase change.
			unmanagedIP := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unmanaged-ip",
					Namespace: testNamespace,
					Annotations: map[string]string{
						osacManagementStateAnnotation: ManagementStateUnmanaged,
					},
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool: testPoolUUID,
				},
			}
			Expect(fakeClient.Create(testCtx, unmanagedIP)).To(Succeed())

			key := types.NamespacedName{Name: unmanagedIP.Name, Namespace: unmanagedIP.Namespace}
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, updated)).To(Succeed())

			Expect(updated.Finalizers).To(BeEmpty())
			Expect(updated.Status.Phase).To(BeEmpty())
		})
	})

	Context("ComputeInstance target namespace annotation", func() {
		const (
			testCINamespace = "osac-computeinstance"
			testCIUUID      = "ci-uuid-456"
			testTenantNS    = "tenant-ns-abc"
		)

		It("should set publicip-target-namespace annotation when computeInstance is set", func() {
			ci := &osacv1alpha1.ComputeInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-ci",
					Namespace: testCINamespace,
					Labels: map[string]string{
						osacComputeInstanceIDLabel: testCIUUID,
					},
				},
			}

			ipWithCI := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-with-ci",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: testCIUUID,
				},
			}

			ciClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(ipWithCI, parentPool, ci).
				WithStatusSubresource(&osacv1alpha1.PublicIP{}, &osacv1alpha1.ComputeInstance{}).
				Build()

			// Apply status separately — WithStatusSubresource strips status from WithObjects.
			ci.Status.VirtualMachineReference = &osacv1alpha1.VirtualMachineReferenceType{
				Namespace:                  testTenantNS,
				KubeVirtVirtualMachineName: "test-vm",
			}
			Expect(ciClient.Status().Update(testCtx, ci)).To(Succeed())

			reconciler.Client = ciClient
			reconciler.APIReader = ciClient
			reconciler.ComputeInstanceNamespace = testCINamespace

			key := types.NamespacedName{Name: ipWithCI.Name, Namespace: ipWithCI.Namespace}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: resolves pool and CI annotations
			_, err = reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(ciClient.Get(testCtx, key, updated)).To(Succeed())
			Expect(updated.Annotations[osacPublicIPTargetNamespaceAnnotation]).To(Equal(testTenantNS))
		})

		It("should clear publicip-target-namespace annotation when computeInstance is cleared", func() {
			ipWithAnnotation := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-clear-ci",
					Namespace: testNamespace,
					Annotations: map[string]string{
						osacPublicIPTargetNamespaceAnnotation: testTenantNS,
					},
					Finalizers: []string{osacPublicIPFinalizer},
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool: testPoolUUID,
				},
			}

			clearClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(ipWithAnnotation, parentPool).
				WithStatusSubresource(&osacv1alpha1.PublicIP{}).
				Build()

			reconciler.Client = clearClient
			reconciler.APIReader = clearClient

			key := types.NamespacedName{Name: ipWithAnnotation.Name, Namespace: ipWithAnnotation.Namespace}

			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			updated := &osacv1alpha1.PublicIP{}
			Expect(clearClient.Get(testCtx, key, updated)).To(Succeed())
			_, exists := updated.Annotations[osacPublicIPTargetNamespaceAnnotation]
			Expect(exists).To(BeFalse())
		})

		It("should requeue when ComputeInstance not found", func() {
			ipMissingCI := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-missing-ci",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: "nonexistent-ci-uuid",
				},
			}

			missingClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(ipMissingCI, parentPool).
				WithStatusSubresource(&osacv1alpha1.PublicIP{}).
				Build()

			reconciler.Client = missingClient
			reconciler.APIReader = missingClient
			reconciler.ComputeInstanceNamespace = testCINamespace

			key := types.NamespacedName{Name: ipMissingCI.Name, Namespace: ipMissingCI.Namespace}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: CI not found, requeue
			result, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(defaultPreconditionRequeueInterval))
		})

		It("should requeue when ComputeInstance has no VirtualMachineReference", func() {
			ciNoTenant := &osacv1alpha1.ComputeInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ci-no-tenant",
					Namespace: testCINamespace,
					Labels: map[string]string{
						osacComputeInstanceIDLabel: testCIUUID,
					},
				},
			}

			ipNoTenant := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-no-tenant",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: testCIUUID,
				},
			}

			noTenantClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(ipNoTenant, parentPool, ciNoTenant).
				WithStatusSubresource(&osacv1alpha1.PublicIP{}, &osacv1alpha1.ComputeInstance{}).
				Build()

			reconciler.Client = noTenantClient
			reconciler.APIReader = noTenantClient
			reconciler.ComputeInstanceNamespace = testCINamespace

			key := types.NamespacedName{Name: ipNoTenant.Name, Namespace: ipNoTenant.Namespace}

			// Pass 1: adds finalizer
			_, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())

			// Pass 2: CI found but no VirtualMachineReference, requeue
			result, err := reconciler.Reconcile(testCtx, mcreconcile.Request{Request: ctrl.Request{NamespacedName: key}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(defaultPreconditionRequeueInterval))
		})
	})

	Context("mapComputeInstanceToPublicIPs", func() {
		const (
			mapTestCINamespace = "osac-computeinstance"
			mapTestCIUUID      = "ci-map-uuid-789"
		)

		It("should return requests for PublicIPs referencing the ComputeInstance UUID", func() {
			matchingIP := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-matching",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: mapTestCIUUID,
				},
			}
			unrelatedIP := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-unrelated",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: "other-ci-uuid",
				},
			}

			mapClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(matchingIP, unrelatedIP).
				Build()

			reconciler.Client = mapClient
			reconciler.NetworkingNamespace = testNamespace
			reconciler.ComputeInstanceNamespace = mapTestCINamespace

			ci := &osacv1alpha1.ComputeInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-ci",
					Namespace: mapTestCINamespace,
					Labels: map[string]string{
						osacComputeInstanceIDLabel: mapTestCIUUID,
					},
				},
			}

			requests := reconciler.mapComputeInstanceToPublicIPs(testCtx, ci)
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].NamespacedName).To(Equal(types.NamespacedName{
				Name:      "ip-matching",
				Namespace: testNamespace,
			}))
		})

		It("should return nil when ComputeInstance has no UUID label", func() {
			ci := &osacv1alpha1.ComputeInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ci-no-label",
					Namespace: mapTestCINamespace,
				},
			}

			requests := reconciler.mapComputeInstanceToPublicIPs(testCtx, ci)
			Expect(requests).To(BeNil())
		})

		It("should return empty when no PublicIPs reference the ComputeInstance", func() {
			unrelatedIP := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-no-match",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: "different-uuid",
				},
			}

			mapClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(unrelatedIP).
				Build()

			reconciler.Client = mapClient
			reconciler.NetworkingNamespace = testNamespace

			ci := &osacv1alpha1.ComputeInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ci-no-matches",
					Namespace: mapTestCINamespace,
					Labels: map[string]string{
						osacComputeInstanceIDLabel: mapTestCIUUID,
					},
				},
			}

			requests := reconciler.mapComputeInstanceToPublicIPs(testCtx, ci)
			Expect(requests).To(BeEmpty())
		})

		It("should return multiple requests when several PublicIPs reference the same ComputeInstance", func() {
			ip1 := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-multi-1",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: mapTestCIUUID,
				},
			}
			ip2 := &osacv1alpha1.PublicIP{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ip-multi-2",
					Namespace: testNamespace,
				},
				Spec: osacv1alpha1.PublicIPSpec{
					Pool:            testPoolUUID,
					ComputeInstance: mapTestCIUUID,
				},
			}

			mapClient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(ip1, ip2).
				Build()

			reconciler.Client = mapClient
			reconciler.NetworkingNamespace = testNamespace

			ci := &osacv1alpha1.ComputeInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ci-multi",
					Namespace: mapTestCINamespace,
					Labels: map[string]string{
						osacComputeInstanceIDLabel: mapTestCIUUID,
					},
				},
			}

			requests := reconciler.mapComputeInstanceToPublicIPs(testCtx, ci)
			Expect(requests).To(HaveLen(2))
			Expect(requests).To(ContainElements(
				reconcile.Request{NamespacedName: types.NamespacedName{Name: "ip-multi-1", Namespace: testNamespace}},
				reconcile.Request{NamespacedName: types.NamespacedName{Name: "ip-multi-2", Namespace: testNamespace}},
			))
		})
	})

	// The provisioning lifecycle uses a config version (hash of spec + strategy) to
	// detect spec changes. When provisioning fails, the controller backs off with
	// exponential delay. But if the spec changes (new config version), it retries
	// immediately instead of waiting for the backoff to expire.
	Context("backoff on failure", func() {
		It("should backoff when latest job failed with matching ConfigVersion", func() {
			publicIP.Status.DesiredConfigVersion = testConfigVersion
			publicIP.Status.Jobs = []osacv1alpha1.JobStatus{
				{
					JobID:         "failed-job",
					Type:          osacv1alpha1.JobTypeProvision,
					Timestamp:     metav1.NewTime(time.Now().UTC()),
					State:         osacv1alpha1.JobStateFailed,
					Message:       "provision failed",
					ConfigVersion: testConfigVersion,
				},
			}

			result, err := reconciler.handleProvisioning(testCtx, publicIP)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			Expect(result.RequeueAfter).To(BeNumerically("<=", provisioning.BackoffMaxDelay))
		})

		It("should trigger immediately when spec changed after failure", func() {
			mockProvider.triggerProvisionFunc = func(
				ctx context.Context, resource client.Object,
			) (*provisioning.ProvisionResult, error) {
				return &provisioning.ProvisionResult{
					JobID:        "retry-job",
					InitialState: osacv1alpha1.JobStatePending,
				}, nil
			}

			// The desired version is "version-2" but the failed job was for "version-1",
			// meaning the spec changed. The controller should retry immediately.
			publicIP.Status.DesiredConfigVersion = testConfigVersionUpdated
			publicIP.Status.Jobs = []osacv1alpha1.JobStatus{
				{
					JobID:         "failed-job",
					Type:          osacv1alpha1.JobTypeProvision,
					Timestamp:     metav1.NewTime(time.Now().UTC().Add(-2 * time.Second)),
					State:         osacv1alpha1.JobStateFailed,
					Message:       "provision failed",
					ConfigVersion: testConfigVersion,
				},
			}

			result, err := reconciler.handleProvisioning(testCtx, publicIP)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(1 * time.Second))

			latestJob := provisioning.FindLatestJobByType(publicIP.Status.Jobs, osacv1alpha1.JobTypeProvision)
			Expect(latestJob).NotTo(BeNil())
			Expect(latestJob.JobID).To(Equal("retry-job"))
		})
	})

	Context("getPublicIPAddress", func() {
		It("should return IP from LoadBalancer Service ingress", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osac-publicip-test-publicip",
					Namespace: "metallb-system",
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{IP: "203.0.113.42"},
						},
					},
				},
			}
			targetClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(svc).Build()

			ip := reconciler.getPublicIPAddress(testCtx, targetClient, "test-publicip")
			Expect(ip).To(Equal("203.0.113.42"))
		})

		It("should return empty string when Service not found", func() {
			targetClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

			ip := reconciler.getPublicIPAddress(testCtx, targetClient, "nonexistent")
			Expect(ip).To(Equal(""))
		})

		It("should return empty string when ingress list is empty", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osac-publicip-test-publicip",
					Namespace: "metallb-system",
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{},
					},
				},
			}
			targetClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(svc).Build()

			ip := reconciler.getPublicIPAddress(testCtx, targetClient, "test-publicip")
			Expect(ip).To(Equal(""))
		})

		It("should return empty string when ingress IP is empty", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osac-publicip-test-publicip",
					Namespace: "metallb-system",
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{IP: ""},
						},
					},
				},
			}
			targetClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(svc).Build()

			ip := reconciler.getPublicIPAddress(testCtx, targetClient, "test-publicip")
			Expect(ip).To(Equal(""))
		})

		It("should not populate address before provisioning succeeds (temporal ordering, ADDR-01)", func() {
			key := types.NamespacedName{Name: publicIP.Name, Namespace: publicIP.Namespace}

			// A LoadBalancer Service with an assigned IP exists on the workload cluster.
			// The guard condition (state==Allocated && address=="") must prevent
			// address population until provisioning has completed.
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osac-publicip-test-publicip",
					Namespace: "metallb-system",
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{IP: "203.0.113.42"},
						},
					},
				},
			}
			targetClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(svc).Build()
			reconciler.mgr = &mockMulticlusterManager{targetClient: targetClient}

			// Keep jobs in Running state so OnSuccess does not fire and
			// override the state we set for each sub-test.
			mockProvider.getProvisionStatusFunc = func(
				_ context.Context, _ client.Object, jobID string,
			) (provisioning.ProvisionStatus, error) {
				return provisioning.ProvisionStatus{
					JobID:   jobID,
					State:   osacv1alpha1.JobStateRunning,
					Message: "Job running",
				}, nil
			}

			// Pending state: address must NOT be populated (pre-provisioning).
			// Call handleUpdate and verify the in-memory object.
			pendingIP := publicIP.DeepCopy()
			pendingIP.Status.Phase = osacv1alpha1.PublicIPPhaseProgressing
			pendingIP.Status.State = osacv1alpha1.PublicIPStatePending
			pendingIP.Status.Address = ""

			_, err := reconciler.handleUpdate(testCtx, pendingIP)
			Expect(err).NotTo(HaveOccurred())
			Expect(pendingIP.Status.Address).To(Equal(""), "address must not be populated in Pending state")

			// Allocated state: address SHOULD be populated (post-provisioning).
			// Re-read the object from the fake client to get the current
			// resourceVersion (handleUpdate modifies the object in the store).
			allocatedIP := &osacv1alpha1.PublicIP{}
			Expect(fakeClient.Get(testCtx, key, allocatedIP)).To(Succeed())
			allocatedIP.Status.State = osacv1alpha1.PublicIPStateAllocated
			allocatedIP.Status.Address = ""

			_, err = reconciler.handleUpdate(testCtx, allocatedIP)
			Expect(err).NotTo(HaveOccurred())
			Expect(allocatedIP.Status.Address).To(Equal("203.0.113.42"), "address should be populated in Allocated state")
		})
	})
})
