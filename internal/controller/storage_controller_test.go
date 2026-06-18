/*
Copyright 2025.

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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/pkg/provisioning"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
)

func storageReconcileRequest(nn types.NamespacedName) mcreconcile.Request {
	return mcreconcile.Request{Request: reconcile.Request{NamespacedName: nn}}
}

func createReadyTenantForStorage(ctx context.Context, name, namespace string) {
	tenant := &v1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	Expect(k8sClient.Create(ctx, tenant)).To(Succeed())

	Eventually(func() error {
		return k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, tenant)
	}, 5*time.Second, 10*time.Millisecond).Should(Succeed())

	tenant.Status.Phase = v1alpha1.TenantPhaseReady
	tenant.Status.Namespace = name
	Expect(k8sClient.Status().Update(ctx, tenant)).To(Succeed())

	Eventually(func(g Gomega) {
		t := &v1alpha1.Tenant{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, t)).To(Succeed())
		g.Expect(t.Status.Phase).To(Equal(v1alpha1.TenantPhaseReady))
	}, 5*time.Second, 10*time.Millisecond).Should(Succeed())
}

func createHubSecret(ctx context.Context, tenantName, namespace string) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("vast-tenant-config-%s", tenantName),
			Namespace: namespace,
			Labels: map[string]string{
				osacTenantKey: tenantName,
			},
		},
		Data: map[string][]byte{
			"vast_tenant_id": []byte("123"),
		},
	}
	Expect(k8sClient.Create(ctx, secret)).To(Succeed())
}

func createLabeledStorageClass(ctx context.Context, name, tenant, tier string) {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				osacTenantKey:        tenant,
				osacStorageTierLabel: tier,
			},
		},
		Provisioner: "kubernetes.io/no-provisioner",
	}
	Expect(k8sClient.Create(ctx, sc)).To(Succeed())
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, sc))).To(Succeed())
	})
}

var _ = Describe("Storage Controller", func() {
	const (
		testNamespace    = "default"
		secretsNamespace = "osac-system"
		pollInterval     = 1 * time.Second
	)

	ctx := context.Background()

	BeforeEach(func() {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: secretsNamespace},
		}
		if err := k8sClient.Create(ctx, ns); err != nil {
			Expect(client.IgnoreAlreadyExists(err)).To(Succeed())
		}
	})

	Context("Stage 1: Backend provisioning", func() {
		It("should skip reconciliation when Tenant is not Ready", func() {
			name := "storage-test-not-ready"
			tenant := &v1alpha1.Tenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: testNamespace,
				},
			}
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				&mockProvisioningProvider{}, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			Eventually(func() error {
				return r.Client.Get(ctx, nn, &v1alpha1.Tenant{})
			}, 5*time.Second, 10*time.Millisecond).Should(Succeed())

			result, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())
			Expect(tenant.Status.Jobs).To(BeEmpty())
			cond := tenant.GetStatusCondition(v1alpha1.TenantConditionStorageBackendReady)
			Expect(cond).To(BeNil())
		})

		It("should trigger backend provisioning when hub Secret is missing", func() {
			name := "storage-test-no-secret"
			createReadyTenantForStorage(ctx, name, testNamespace)

			provider := &mockProvisioningProvider{}
			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				provider, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			result, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(pollInterval))

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())
			Expect(tenant.Status.Jobs).To(HaveLen(1))
			Expect(tenant.Status.Jobs[0].Type).To(Equal(v1alpha1.JobTypeStorageBackendProvision))
			Expect(tenant.Status.Jobs[0].JobID).To(Equal("mock-job-id"))

			cond := tenant.GetStatusCondition(v1alpha1.TenantConditionStorageBackendReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		})

		It("should set StorageBackendReady=True when hub Secret exists", func() {
			name := "storage-test-secret-exists"
			createReadyTenantForStorage(ctx, name, testNamespace)
			createHubSecret(ctx, name, secretsNamespace)

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())

			cond := tenant.GetStatusCondition(v1alpha1.TenantConditionStorageBackendReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(v1alpha1.TenantReasonFound))
		})

		It("should set StorageBackendReady=False with NoProvider when no provider configured", func() {
			name := "storage-test-no-provider"
			createReadyTenantForStorage(ctx, name, testNamespace)

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())

			cond := tenant.GetStatusCondition(v1alpha1.TenantConditionStorageBackendReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("NoProvider"))
		})

		It("should record failed provisioning job", func() {
			name := "storage-test-prov-fail"
			createReadyTenantForStorage(ctx, name, testNamespace)

			provider := &mockProvisioningProvider{
				triggerProvisionFunc: func(_ context.Context, _ client.Object) (*provisioning.ProvisionResult, error) {
					return nil, fmt.Errorf("AAP unreachable")
				},
			}
			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				provider, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())
			Expect(tenant.Status.Jobs).To(HaveLen(1))
			Expect(tenant.Status.Jobs[0].State).To(Equal(v1alpha1.JobStateFailed))
			Expect(tenant.Status.Jobs[0].Message).To(ContainSubstring("AAP unreachable"))
		})
	})

	Context("Stage 2: Cluster storage provisioning", func() {
		It("should trigger cluster storage provisioning when Stage 1 complete but no SCs", func() {
			name := "storage-test-no-sc"
			createReadyTenantForStorage(ctx, name, testNamespace)
			createHubSecret(ctx, name, secretsNamespace)

			clusterProvider := &mockProvisioningProvider{name: "cluster-storage-mock"}
			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, clusterProvider, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			result, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(pollInterval))

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())

			backendCond := tenant.GetStatusCondition(v1alpha1.TenantConditionStorageBackendReady)
			Expect(backendCond).NotTo(BeNil())
			Expect(backendCond.Status).To(Equal(metav1.ConditionTrue))

			Expect(tenant.Status.Jobs).To(HaveLen(1))
			Expect(tenant.Status.Jobs[0].Type).To(Equal(v1alpha1.JobTypeClusterStorageProvision))
		})

		It("should set ClusterStorageReady=True when SCs are discovered", func() {
			name := "storage-test-sc-found"
			createReadyTenantForStorage(ctx, name, testNamespace)
			createHubSecret(ctx, name, secretsNamespace)
			createLabeledStorageClass(ctx, name+"-default-sc", name, "default")

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())

			clusterCond := tenant.GetStatusCondition(v1alpha1.TenantConditionClusterStorageReady)
			Expect(clusterCond).NotTo(BeNil())
			Expect(clusterCond.Status).To(Equal(metav1.ConditionTrue))

			Expect(tenant.Status.StorageClasses).To(HaveLen(1))
			Expect(tenant.Status.StorageClasses[0].Name).To(Equal(name + "-default-sc"))
			Expect(tenant.Status.StorageClasses[0].Tier).To(Equal("default"))
		})
	})

	Context("Tier resolution", func() {
		It("should fall back to Default StorageClass when no tenant-specific SC", func() {
			name := "storage-test-default-fallback"
			createReadyTenantForStorage(ctx, name, testNamespace)
			createHubSecret(ctx, name, secretsNamespace)
			createLabeledStorageClass(ctx, "shared-default-sc-"+name, defaultStorageClassSentinel, "default")

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())

			Expect(tenant.Status.StorageClasses).To(HaveLen(1))
			Expect(tenant.Status.StorageClasses[0].Name).To(Equal("shared-default-sc-" + name))
		})

		It("should prefer tenant-specific SC over Default", func() {
			name := "storage-test-tenant-priority"
			createReadyTenantForStorage(ctx, name, testNamespace)
			createHubSecret(ctx, name, secretsNamespace)
			createLabeledStorageClass(ctx, "shared-sc-"+name, defaultStorageClassSentinel, "default")
			createLabeledStorageClass(ctx, name+"-tenant-sc", name, "default")

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())

			Expect(tenant.Status.StorageClasses).To(HaveLen(1))
			Expect(tenant.Status.StorageClasses[0].Name).To(Equal(name + "-tenant-sc"))
		})
	})

	Context("Finalizer and deletion", func() {
		It("should add storage finalizer on first reconcile", func() {
			name := "storage-test-finalizer"
			createReadyTenantForStorage(ctx, name, testNamespace)

			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				nil, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())
			Expect(tenant.Finalizers).To(ContainElement(storageFinalizer))
		})

		It("should run deletion even when class provider is nil", func() {
			name := "storage-test-delete-no-class"
			createReadyTenantForStorage(ctx, name, testNamespace)

			backendProvider := &mockProvisioningProvider{name: "backend-mock"}
			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				backendProvider, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())

			tenant := &v1alpha1.Tenant{}
			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())
			Expect(tenant.Finalizers).To(ContainElement(storageFinalizer))

			Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())

			Eventually(func(g Gomega) {
				_, err := r.Reconcile(ctx, storageReconcileRequest(nn))
				g.Expect(err).NotTo(HaveOccurred())

				t := &v1alpha1.Tenant{}
				err = k8sClient.Get(ctx, nn, t)
				g.Expect(client.IgnoreNotFound(err)).To(Succeed())
				if err == nil {
					g.Expect(t.Finalizers).NotTo(ContainElement(storageFinalizer))
				}
			}).Should(Succeed())
		})
	})

	Context("Management state", func() {
		It("should skip reconciliation when Unmanaged", func() {
			name := "storage-test-unmanaged"
			tenant := &v1alpha1.Tenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: testNamespace,
					Annotations: map[string]string{
						osacManagementStateAnnotation: ManagementStateUnmanaged,
					},
				},
			}
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())

			Eventually(func() error {
				t := &v1alpha1.Tenant{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, t)
			}, 5*time.Second, 10*time.Millisecond).Should(Succeed())

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, tenant)).To(Succeed())
			tenant.Status.Phase = v1alpha1.TenantPhaseReady
			Expect(k8sClient.Status().Update(ctx, tenant)).To(Succeed())

			provider := &mockProvisioningProvider{}
			r := NewStorageReconciler(
				testMcManager, testNamespace, mcmanager.LocalCluster,
				provider, nil, pollInterval,
				provisioning.DefaultMaxJobHistory,
			)

			nn := types.NamespacedName{Name: name, Namespace: testNamespace}
			result, err := r.Reconcile(ctx, storageReconcileRequest(nn))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			Expect(k8sClient.Get(ctx, nn, tenant)).To(Succeed())
			Expect(tenant.Status.Jobs).To(BeEmpty())
			Expect(tenant.Finalizers).NotTo(ContainElement(storageFinalizer))
		})
	})
})
