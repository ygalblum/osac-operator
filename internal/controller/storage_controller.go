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
	"errors"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mc "sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/pkg/provisioning"
)

const (
	storageFinalizer      = "osac.openshift.io/storage"
	storageControllerName = "storage-controller"
)

// StorageReconciler reconciles storage lifecycle on Tenant CRs.
// It owns StorageBackendReady, ClusterStorageReady conditions,
// status.storageClasses, and status.jobs on the Tenant CR.
type StorageReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	Recorder               events.EventRecorder
	tenantNamespace        string
	mgr                    mcmanager.Manager
	targetCluster          mc.ClusterName
	BackendProvider        provisioning.ProvisioningProvider
	ClusterStorageProvider provisioning.ProvisioningProvider
	StatusPollInterval     time.Duration
	MaxJobHistory          int
}

// +kubebuilder:rbac:groups=osac.openshift.io,resources=tenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=tenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=osac.openshift.io,resources=clusterorders,verbs=get;list;watch
// Secrets RBAC is scoped to osac-system via a namespaced Role (config/rbac/storage_secrets_role.yaml)
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func NewStorageReconciler(
	mgr mcmanager.Manager,
	tenantNamespace string,
	targetCluster mc.ClusterName,
	backendProvider provisioning.ProvisioningProvider,
	clusterStorageProvider provisioning.ProvisioningProvider,
	statusPollInterval time.Duration,
	maxJobHistory int,
) *StorageReconciler {
	if mgr == nil {
		panic("mgr must not be nil")
	}

	if statusPollInterval == 0 {
		statusPollInterval = 30 * time.Second
	}

	if maxJobHistory <= 0 {
		maxJobHistory = provisioning.DefaultMaxJobHistory
	}

	return &StorageReconciler{
		Client:                 mgr.GetLocalManager().GetClient(),
		Scheme:                 mgr.GetLocalManager().GetScheme(),
		Recorder:               mgr.GetLocalManager().GetEventRecorder(storageControllerName),
		tenantNamespace:        tenantNamespace,
		mgr:                    mgr,
		targetCluster:          targetCluster,
		BackendProvider:        backendProvider,
		ClusterStorageProvider: clusterStorageProvider,
		StatusPollInterval:     statusPollInterval,
		MaxJobHistory:          maxJobHistory,
	}
}

func (r *StorageReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	instance := &v1alpha1.Tenant{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if val, exists := instance.Annotations[osacManagementStateAnnotation]; instance.ObjectMeta.DeletionTimestamp.IsZero() && exists && val == ManagementStateUnmanaged {
		log.Info("skipping storage reconciliation — management state is Unmanaged")
		return ctrl.Result{}, nil
	}

	if instance.Status.Phase != v1alpha1.TenantPhaseReady && instance.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	log.Info("start storage reconcile")

	oldstatus := instance.Status.DeepCopy()

	var res ctrl.Result
	var err error
	if instance.ObjectMeta.DeletionTimestamp.IsZero() {
		res, err = r.handleUpdate(ctx, instance)
	} else {
		res, err = r.handleDelete(ctx, instance)
	}

	if !equality.Semantic.DeepEqual(instance.Status, *oldstatus) {
		log.Info("storage status requires update")
		if updateErr := r.Status().Update(ctx, instance); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
	}

	log.Info("end storage reconcile")
	return res, err
}

func (r *StorageReconciler) handleUpdate(ctx context.Context, instance *v1alpha1.Tenant) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)
	tenantName := instance.GetName()

	log.Info("handling storage update for Tenant", "name", tenantName)

	if !controllerutil.ContainsFinalizer(instance, storageFinalizer) {
		controllerutil.AddFinalizer(instance, storageFinalizer)
		if err := r.Update(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Stage 1: check hub Secret
	hubSecretReady, err := r.hubSecretExists(ctx, tenantName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !hubSecretReady {
		instance.SetStatusCondition(v1alpha1.TenantConditionStorageBackendReady,
			metav1.ConditionFalse,
			v1alpha1.TenantReasonNotFound,
			fmt.Sprintf("Hub Secret for tenant %q not found", tenantName))

		if r.BackendProvider != nil {
			return r.handleBackendProvisioning(ctx, instance)
		}
		instance.SetStatusCondition(v1alpha1.TenantConditionStorageBackendReady,
			metav1.ConditionFalse,
			"NoProvider",
			"No backend provider configured")
		return ctrl.Result{}, nil
	}

	instance.SetStatusCondition(v1alpha1.TenantConditionStorageBackendReady,
		metav1.ConditionTrue,
		v1alpha1.TenantReasonFound,
		fmt.Sprintf("Hub Secret for tenant %q exists", tenantName))

	// Stage 2: resolve StorageClasses on target cluster
	targetClient, err := getTargetClient(ctx, r.mgr, r.targetCluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	result, err := getTenantStorageClasses(ctx, targetClient, tenantName)
	if err != nil {
		return ctrl.Result{}, err
	}

	for _, msg := range result.duplicateMessages {
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, eventReasonDuplicateStorageClass, eventActionDetectDuplicate, "%s", msg)
	}

	if len(result.resolved) == 0 {
		reason := v1alpha1.TenantReasonNotFound
		if len(result.duplicateMessages) > 0 {
			reason = v1alpha1.TenantReasonMultipleFound
		}
		condMsg := result.conditionMessage()
		instance.SetStatusCondition(v1alpha1.TenantConditionClusterStorageReady,
			metav1.ConditionFalse,
			reason,
			condMsg)

		if r.ClusterStorageProvider != nil && reason == v1alpha1.TenantReasonNotFound {
			return r.handleClusterStorageProvisioning(ctx, instance)
		}
		return ctrl.Result{}, nil
	}

	instance.SetStatusCondition(v1alpha1.TenantConditionClusterStorageReady,
		metav1.ConditionTrue,
		v1alpha1.TenantReasonFound,
		result.conditionMessage())
	instance.Status.StorageClasses = result.resolved

	// Poll any non-terminal class provision job before declaring complete
	latestClassJob := provisioning.FindLatestJobByType(instance.Status.Jobs, v1alpha1.JobTypeClusterStorageProvision)
	if latestClassJob != nil && !latestClassJob.State.IsTerminal() && r.ClusterStorageProvider != nil {
		return r.pollClusterStorageProvisionJob(ctx, instance, latestClassJob)
	}

	return ctrl.Result{}, nil
}

func (r *StorageReconciler) handleDelete(ctx context.Context, instance *v1alpha1.Tenant) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)
	log.Info("handling storage delete for Tenant", "name", instance.Name)

	if !controllerutil.ContainsFinalizer(instance, storageFinalizer) {
		return ctrl.Result{}, nil
	}

	// Stage 1: class cleanup
	classDeprovJob := provisioning.FindLatestJobByType(instance.Status.Jobs, v1alpha1.JobTypeClusterStorageDeprovision)
	classCleanupDone := classDeprovJob != nil && classDeprovJob.State.IsTerminal() && classDeprovJob.State.IsSuccessful()

	if !classCleanupDone {
		result, err := r.handleClusterStorageDeprovisioning(ctx, instance)
		if err != nil {
			return result, err
		}
		if result.RequeueAfter > 0 {
			return result, nil
		}
		// Class cleanup just completed successfully — fall through to backend
	}

	// Stage 2: backend teardown
	result, err := r.handleBackendDeprovisioning(ctx, instance)
	if err != nil {
		return result, err
	}
	if result.RequeueAfter > 0 {
		return result, nil
	}

	controllerutil.RemoveFinalizer(instance, storageFinalizer)
	if err := r.Update(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("storage finalizer removed, deletion will proceed")
	return ctrl.Result{}, nil
}

// --- Stage 1: Backend provisioning ---

func (r *StorageReconciler) handleBackendProvisioning(ctx context.Context, instance *v1alpha1.Tenant) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	latestJob := provisioning.FindLatestJobByType(instance.Status.Jobs, v1alpha1.JobTypeStorageBackendProvision)

	if latestJob != nil && latestJob.State == v1alpha1.JobStateFailed {
		log.Info("latest backend provision job failed, waiting for external trigger to retry",
			"message", latestJob.Message)
		return ctrl.Result{}, nil
	}

	if provisioning.NeedsProvisionJob(latestJob) {
		log.Info("triggering backend provisioning", "provider", r.BackendProvider.Name())
		result, err := r.BackendProvider.TriggerProvision(ctx, instance)
		if err != nil {
			var rateLimitErr *provisioning.RateLimitError
			if errors.As(err, &rateLimitErr) {
				log.Info("backend provisioning rate-limited, will retry", "retryAfter", rateLimitErr.RetryAfter)
				return ctrl.Result{RequeueAfter: rateLimitErr.RetryAfter}, nil
			}

			log.Error(err, "failed to trigger backend provisioning")
			newJob := v1alpha1.JobStatus{
				JobID:     "",
				Type:      v1alpha1.JobTypeStorageBackendProvision,
				Timestamp: metav1.NewTime(time.Now().UTC()),
				State:     v1alpha1.JobStateFailed,
				Message:   fmt.Sprintf("Failed to trigger backend provisioning: %v", err),
			}
			instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}

		newJob := v1alpha1.JobStatus{
			JobID:     result.JobID,
			Type:      v1alpha1.JobTypeStorageBackendProvision,
			Timestamp: metav1.NewTime(time.Now().UTC()),
			State:     result.InitialState,
			Message:   result.Message,
		}
		instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
		log.Info("backend provisioning job triggered", "jobID", result.JobID)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	return r.pollBackendProvisionJob(ctx, instance, latestJob)
}

func (r *StorageReconciler) pollBackendProvisionJob(ctx context.Context, instance *v1alpha1.Tenant, latestJob *v1alpha1.JobStatus) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	status, err := r.BackendProvider.GetProvisionStatus(ctx, instance, latestJob.JobID)
	if err != nil {
		log.Error(err, "failed to get backend provision job status", "jobID", latestJob.JobID)
		updatedJob := *latestJob
		updatedJob.Message = fmt.Sprintf("Failed to get job status: %v", err)
		provisioning.UpdateJob(instance.Status.Jobs, updatedJob)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	updatedJob := *latestJob
	updatedJob.State = status.State
	updatedJob.Message = status.MessageWithDetails()
	provisioning.UpdateJob(instance.Status.Jobs, updatedJob)

	if !status.State.IsTerminal() {
		log.Info("backend provisioning job still running", "jobID", latestJob.JobID, "state", status.State)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	if status.State.IsSuccessful() {
		log.Info("backend provisioning job succeeded, requeueing to confirm hub Secret", "jobID", latestJob.JobID)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	log.Info("backend provisioning job failed", "jobID", latestJob.JobID, "message", updatedJob.Message)
	return ctrl.Result{}, nil
}

// --- Stage 2: Cluster storage provisioning ---

func (r *StorageReconciler) handleClusterStorageProvisioning(ctx context.Context, instance *v1alpha1.Tenant) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	latestJob := provisioning.FindLatestJobByType(instance.Status.Jobs, v1alpha1.JobTypeClusterStorageProvision)

	if latestJob != nil && latestJob.State == v1alpha1.JobStateFailed {
		log.Info("latest class provision job failed, waiting for external trigger to retry",
			"message", latestJob.Message)
		return ctrl.Result{}, nil
	}

	if provisioning.NeedsProvisionJob(latestJob) {
		log.Info("triggering class provisioning", "provider", r.ClusterStorageProvider.Name())
		result, err := r.ClusterStorageProvider.TriggerProvision(ctx, instance)
		if err != nil {
			var rateLimitErr *provisioning.RateLimitError
			if errors.As(err, &rateLimitErr) {
				log.Info("class provisioning rate-limited, will retry", "retryAfter", rateLimitErr.RetryAfter)
				return ctrl.Result{RequeueAfter: rateLimitErr.RetryAfter}, nil
			}

			log.Error(err, "failed to trigger class provisioning")
			newJob := v1alpha1.JobStatus{
				JobID:     "",
				Type:      v1alpha1.JobTypeClusterStorageProvision,
				Timestamp: metav1.NewTime(time.Now().UTC()),
				State:     v1alpha1.JobStateFailed,
				Message:   fmt.Sprintf("Failed to trigger class provisioning: %v", err),
			}
			instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}

		newJob := v1alpha1.JobStatus{
			JobID:     result.JobID,
			Type:      v1alpha1.JobTypeClusterStorageProvision,
			Timestamp: metav1.NewTime(time.Now().UTC()),
			State:     result.InitialState,
			Message:   result.Message,
		}
		instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
		log.Info("class provisioning job triggered", "jobID", result.JobID)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	return r.pollClusterStorageProvisionJob(ctx, instance, latestJob)
}

func (r *StorageReconciler) pollClusterStorageProvisionJob(ctx context.Context, instance *v1alpha1.Tenant, latestJob *v1alpha1.JobStatus) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	status, err := r.ClusterStorageProvider.GetProvisionStatus(ctx, instance, latestJob.JobID)
	if err != nil {
		log.Error(err, "failed to get class provision job status", "jobID", latestJob.JobID)
		updatedJob := *latestJob
		updatedJob.Message = fmt.Sprintf("Failed to get job status: %v", err)
		provisioning.UpdateJob(instance.Status.Jobs, updatedJob)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	updatedJob := *latestJob
	updatedJob.State = status.State
	updatedJob.Message = status.MessageWithDetails()
	provisioning.UpdateJob(instance.Status.Jobs, updatedJob)

	if !status.State.IsTerminal() {
		log.Info("class provisioning job still running", "jobID", latestJob.JobID, "state", status.State)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	if status.State.IsSuccessful() {
		log.Info("class provisioning job succeeded, requeueing to discover StorageClasses", "jobID", latestJob.JobID)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	log.Info("class provisioning job failed", "jobID", latestJob.JobID, "message", updatedJob.Message)
	return ctrl.Result{}, nil
}

// --- Deprovisioning ---

func (r *StorageReconciler) handleClusterStorageDeprovisioning(ctx context.Context, instance *v1alpha1.Tenant) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	if r.ClusterStorageProvider == nil {
		log.Info("no class provider configured, skipping cluster-side cleanup")
		return ctrl.Result{}, nil
	}

	latestJob := provisioning.FindLatestJobByType(instance.Status.Jobs, v1alpha1.JobTypeClusterStorageDeprovision)

	if latestJob == nil || latestJob.JobID == "" {
		log.Info("triggering class deprovisioning", "provider", r.ClusterStorageProvider.Name())
		result, err := r.ClusterStorageProvider.TriggerDeprovision(ctx, instance)
		if err != nil {
			var rateLimitErr *provisioning.RateLimitError
			if errors.As(err, &rateLimitErr) {
				return ctrl.Result{RequeueAfter: rateLimitErr.RetryAfter}, nil
			}
			log.Error(err, "failed to trigger class deprovisioning")
			newJob := v1alpha1.JobStatus{
				JobID:     "",
				Type:      v1alpha1.JobTypeClusterStorageDeprovision,
				Timestamp: metav1.NewTime(time.Now().UTC()),
				State:     v1alpha1.JobStateFailed,
				Message:   fmt.Sprintf("Failed to trigger class deprovisioning: %v", err),
			}
			instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}

		switch result.Action {
		case provisioning.DeprovisionTriggered:
			newJob := v1alpha1.JobStatus{
				JobID:                  result.JobID,
				Type:                   v1alpha1.JobTypeClusterStorageDeprovision,
				Timestamp:              metav1.NewTime(time.Now().UTC()),
				State:                  v1alpha1.JobStatePending,
				Message:                "Class deprovisioning job triggered",
				BlockDeletionOnFailure: result.BlockDeletionOnFailure,
			}
			instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		case provisioning.DeprovisionSkipped:
			return ctrl.Result{}, nil
		case provisioning.DeprovisionWaiting:
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		default:
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}
	}

	return r.pollDeprovisionJob(ctx, instance, latestJob, r.ClusterStorageProvider)
}

func (r *StorageReconciler) handleBackendDeprovisioning(ctx context.Context, instance *v1alpha1.Tenant) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	if r.BackendProvider == nil {
		log.Info("no backend provider configured, skipping backend teardown")
		return ctrl.Result{}, nil
	}

	hubSecretReady, err := r.hubSecretExists(ctx, instance.GetName())
	if err != nil {
		log.Error(err, "failed to check hub Secret existence, requeueing")
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	latestJob := provisioning.FindLatestJobByType(instance.Status.Jobs, v1alpha1.JobTypeStorageBackendDeprovision)
	deprovJobRunning := latestJob != nil && latestJob.JobID != "" && !latestJob.State.IsTerminal()
	deprovJobFailedBlocking := latestJob != nil &&
		latestJob.State.IsTerminal() &&
		!latestJob.State.IsSuccessful() &&
		latestJob.BlockDeletionOnFailure

	if !hubSecretReady && !deprovJobRunning && !deprovJobFailedBlocking {
		return ctrl.Result{}, nil
	}

	if latestJob == nil || latestJob.JobID == "" {
		log.Info("triggering backend deprovisioning", "provider", r.BackendProvider.Name())
		result, err := r.BackendProvider.TriggerDeprovision(ctx, instance)
		if err != nil {
			var rateLimitErr *provisioning.RateLimitError
			if errors.As(err, &rateLimitErr) {
				return ctrl.Result{RequeueAfter: rateLimitErr.RetryAfter}, nil
			}
			log.Error(err, "failed to trigger backend deprovisioning")
			newJob := v1alpha1.JobStatus{
				JobID:     "",
				Type:      v1alpha1.JobTypeStorageBackendDeprovision,
				Timestamp: metav1.NewTime(time.Now().UTC()),
				State:     v1alpha1.JobStateFailed,
				Message:   fmt.Sprintf("Failed to trigger backend deprovisioning: %v", err),
			}
			instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}

		switch result.Action {
		case provisioning.DeprovisionTriggered:
			newJob := v1alpha1.JobStatus{
				JobID:                  result.JobID,
				Type:                   v1alpha1.JobTypeStorageBackendDeprovision,
				Timestamp:              metav1.NewTime(time.Now().UTC()),
				State:                  v1alpha1.JobStatePending,
				Message:                "Backend deprovisioning job triggered",
				BlockDeletionOnFailure: result.BlockDeletionOnFailure,
			}
			instance.Status.Jobs = provisioning.AppendJob(instance.Status.Jobs, newJob, r.MaxJobHistory)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		case provisioning.DeprovisionSkipped:
			return ctrl.Result{}, nil
		case provisioning.DeprovisionWaiting:
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		default:
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}
	}

	return r.pollDeprovisionJob(ctx, instance, latestJob, r.BackendProvider)
}

func (r *StorageReconciler) pollDeprovisionJob(ctx context.Context, instance *v1alpha1.Tenant, latestJob *v1alpha1.JobStatus, provider provisioning.ProvisioningProvider) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	if latestJob.State.IsTerminal() {
		if !latestJob.State.IsSuccessful() && latestJob.BlockDeletionOnFailure {
			log.Info("deprovisioning job failed, blocking deletion",
				"jobID", latestJob.JobID, "state", latestJob.State)
			return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
		}
		return ctrl.Result{}, nil
	}

	status, err := provider.GetDeprovisionStatus(ctx, instance, latestJob.JobID)
	if err != nil {
		log.Error(err, "failed to get deprovision job status", "jobID", latestJob.JobID)
		updatedJob := *latestJob
		updatedJob.Message = fmt.Sprintf("Failed to get job status: %v", err)
		provisioning.UpdateJob(instance.Status.Jobs, updatedJob)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	updatedJob := *latestJob
	updatedJob.State = status.State
	updatedJob.Message = status.MessageWithDetails()
	provisioning.UpdateJob(instance.Status.Jobs, updatedJob)

	if !status.State.IsTerminal() {
		log.Info("deprovisioning job still running", "jobID", latestJob.JobID, "state", status.State)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	if status.State.IsSuccessful() {
		log.Info("deprovisioning job succeeded", "jobID", latestJob.JobID)
		return ctrl.Result{}, nil
	}

	if latestJob.BlockDeletionOnFailure {
		log.Info("deprovisioning job failed, blocking deletion",
			"jobID", latestJob.JobID, "message", updatedJob.Message)
		return ctrl.Result{RequeueAfter: r.StatusPollInterval}, nil
	}

	log.Info("deprovisioning job failed, continuing with deletion",
		"jobID", latestJob.JobID, "message", updatedJob.Message)
	return ctrl.Result{}, nil
}

// --- Helpers ---

func (r *StorageReconciler) hubSecretExists(ctx context.Context, tenantName string) (bool, error) {
	var secretList corev1.SecretList
	if err := r.List(ctx, &secretList,
		client.InNamespace(storageConfigNamespace()),
		client.MatchingLabels{osacTenantKey: tenantName},
	); err != nil {
		return false, err
	}
	return len(secretList.Items) > 0, nil
}

func storageConfigNamespace() string {
	if ns := os.Getenv("OSAC_STORAGE_CONFIG_NAMESPACE"); ns != "" {
		return ns
	}
	return "osac-system"
}

// --- Watch mapping ---

func (r *StorageReconciler) mapStorageClassToTenant(ctx context.Context, obj client.Object) []reconcile.Request {
	log := ctrllog.FromContext(ctx)

	tenantName, exists := obj.GetLabels()[osacTenantKey]
	if !exists || tenantName == "" {
		return nil
	}

	if tenantName == defaultStorageClassSentinel {
		log.Info("shared Default StorageClass changed, reconciling all tenants",
			"storageClass", obj.GetName())
		return r.allTenantReconcileRequests(ctx)
	}

	tenant := &v1alpha1.Tenant{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.tenantNamespace, Name: tenantName}, tenant); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to get Tenant for StorageClass",
				"storageClass", obj.GetName(), "tenant", tenantName)
		}
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(tenant)}}
}

func (r *StorageReconciler) mapSecretToTenant(ctx context.Context, obj client.Object) []reconcile.Request {
	tenantName, exists := obj.GetLabels()[osacTenantKey]
	if !exists || tenantName == "" {
		return nil
	}

	tenant := &v1alpha1.Tenant{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.tenantNamespace, Name: tenantName}, tenant); err != nil {
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(tenant)}}
}

func (r *StorageReconciler) allTenantReconcileRequests(ctx context.Context) []reconcile.Request {
	log := ctrllog.FromContext(ctx)

	tenantList := &v1alpha1.TenantList{}
	if err := r.List(ctx, tenantList, client.InNamespace(r.tenantNamespace)); err != nil {
		log.Error(err, "unable to list Tenants for Default SC reconciliation")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(tenantList.Items))
	for i := range tenantList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&tenantList.Items[i]),
		})
	}
	return requests
}

// TODO(OSAC-1123): implement ClusterOrder-to-Tenant mapping when CaaS defines the association
func (r *StorageReconciler) mapClusterOrderToTenant(_ context.Context, _ client.Object) []reconcile.Request {
	return nil
}

func (r *StorageReconciler) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&v1alpha1.Tenant{},
			mcbuilder.WithPredicates(tenantNamespacePredicate(r.tenantNamespace)),
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(false)).
		Named("storage").
		Watches(
			&v1alpha1.ClusterOrder{},
			mchandler.EnqueueRequestsFromMapFunc(r.mapClusterOrderToTenant),
		).
		Watches(
			&storagev1.StorageClass{},
			mchandler.EnqueueRequestsFromMapFunc(r.mapStorageClassToTenant),
			mcbuilder.WithPredicates(storageClassTenantPredicate()),
		).
		Watches(
			&corev1.Secret{},
			mchandler.EnqueueRequestsFromMapFunc(r.mapSecretToTenant),
			mcbuilder.WithPredicates(secretTenantPredicate()),
		).
		Complete(r)
}

func secretTenantPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		if obj.GetNamespace() != storageConfigNamespace() {
			return false
		}
		_, exists := obj.GetLabels()[osacTenantKey]
		return exists
	})
}
