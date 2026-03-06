/*
Copyright 2024 The Kubernetes Authors.

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

package cloudprovider

import (
	"cmp"
	"context"
	_ "embed"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	capiv1beta1 "sigs.k8s.io/cluster-api/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter-provider-cluster-api/pkg/apis/v1alpha1"
	"sigs.k8s.io/karpenter-provider-cluster-api/pkg/providers"
	"sigs.k8s.io/karpenter-provider-cluster-api/pkg/providers/machine"
	"sigs.k8s.io/karpenter-provider-cluster-api/pkg/providers/machinedeployment"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const (
	// TODO (elmiko) if we make these exposed constants from the CAS we can import them instead of redifining and risking drift
	cpuKey          = "capacity.cluster-autoscaler.kubernetes.io/cpu"
	memoryKey       = "capacity.cluster-autoscaler.kubernetes.io/memory"
	gpuCountKey     = "capacity.cluster-autoscaler.kubernetes.io/gpu-count"
	gpuTypeKey      = "capacity.cluster-autoscaler.kubernetes.io/gpu-type"
	diskCapacityKey = "capacity.cluster-autoscaler.kubernetes.io/ephemeral-disk"
	labelsKey       = "capacity.cluster-autoscaler.kubernetes.io/labels"
	taintsKey       = "capacity.cluster-autoscaler.kubernetes.io/taints"
	maxPodsKey      = "capacity.cluster-autoscaler.kubernetes.io/maxPods"

	machineAnnotation           = "cluster.x-k8s.io/machine"
	machineDeploymentAnnotation = "cluster.x-k8s.io/machine-deployment"

	// pendingProviderIDPrefix is used as a synthetic ProviderID when a Machine has been claimed
	// by karpenter but its real ProviderID has not yet been assigned by the infrastructure provider.
	// It encodes the Machine namespace/name so that Delete and Get can locate the Machine.
	// Without a non-empty ProviderID, karpenter's finalize() skips calling our Delete, so the
	// MachineDeployment replica count would never be decremented on NodeClaim termination.
	pendingProviderIDPrefix = "capi-pending://"
)

func NewCloudProvider(ctx context.Context, kubeClient client.Client, machineProvider machine.Provider, machineDeploymentProvider machinedeployment.Provider) *CloudProvider {
	return &CloudProvider{
		kubeClient:                kubeClient,
		machineProvider:           machineProvider,
		machineDeploymentProvider: machineDeploymentProvider,
	}
}

type ClusterAPIInstanceType struct {
	cloudprovider.InstanceType

	MachineDeploymentName      string
	MachineDeploymentNamespace string
}

type CloudProvider struct {
	kubeClient                client.Client
	accessLock                sync.Mutex
	machineProvider           machine.Provider
	machineDeploymentProvider machinedeployment.Provider
}

func (c *CloudProvider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	// to eliminate racing if multiple creation occur, we gate access to this function
	c.accessLock.Lock()
	defer c.accessLock.Unlock()

	if nodeClaim == nil {
		return nil, fmt.Errorf("cannot satisfy create, NodeClaim is nil")
	}

	machineDeployment, machine, err := c.provisionMachine(ctx, nodeClaim)
	if err != nil {
		return nil, err
	}

	//  fill out nodeclaim with details
	createdNodeClaim := createNodeClaimFromMachineDeployment(machineDeployment)
	if machine.Spec.ProviderID != nil && *machine.Spec.ProviderID != "" {
		createdNodeClaim.Status.ProviderID = *machine.Spec.ProviderID
	} else {
		// Machine is claimed but the infrastructure provider hasn't assigned a ProviderID yet.
		// Return a synthetic ProviderID so karpenter marks this NodeClaim as "launched".
		// Without a non-empty ProviderID, karpenter's finalize() skips calling our Delete when
		// the NodeClaim is terminated, leaving the MachineDeployment replica count permanently
		// elevated (the root cause of the replica escalation bug).
		// The real ProviderID will replace this once the infrastructure provisions the Machine.
		createdNodeClaim.Status.ProviderID = fmt.Sprintf("%s%s/%s", pendingProviderIDPrefix, machine.Namespace, machine.Name)
	}

	return createdNodeClaim, nil
}

func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *karpv1.NodeClaim) error {
	// to eliminate racing if multiple deletion occur, we gate access to this function
	c.accessLock.Lock()
	defer c.accessLock.Unlock()

	var machine *capiv1beta1.Machine
	var err error

	// find machine
	if len(nodeClaim.Status.ProviderID) != 0 {
		if strings.HasPrefix(nodeClaim.Status.ProviderID, pendingProviderIDPrefix) {
			// NodeClaim was launched with a synthetic ProviderID before the Machine received its
			// real one. Extract the Machine namespace/name encoded in the synthetic ProviderID.
			ref := strings.TrimPrefix(nodeClaim.Status.ProviderID, pendingProviderIDPrefix)
			machineNamespace, machineName, parseErr := parseMachineAnnotation(ref)
			if parseErr != nil {
				return fmt.Errorf("error parsing pending provider ID %q for NodeClaim %q: %w", nodeClaim.Status.ProviderID, nodeClaim.Name, parseErr)
			}
			machine, err = c.machineProvider.Get(ctx, machineName, machineNamespace)
			if err != nil {
				if apierrors.IsNotFound(err) {
					if mdAnno, ok := nodeClaim.Annotations[machineDeploymentAnnotation]; ok {
						if mdNamespace, mdName, parseErr := parseMachineAnnotation(mdAnno); parseErr == nil {
							c.tryDecrementMachineDeploymentReplicas(ctx, mdNamespace, mdName)
						}
					}
					return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("Machine %q in namespace %q no longer exists for NodeClaim %q", machineName, machineNamespace, nodeClaim.Name))
				}
				return fmt.Errorf("error finding Machine %q to Delete NodeClaim %q: %w", machineName, nodeClaim.Name, err)
			}
		} else {
			machine, err = c.machineProvider.GetByProviderID(ctx, nodeClaim.Status.ProviderID)
			if err != nil {
				return fmt.Errorf("error finding Machine with provider ID %q to Delete NodeClaim %q: %w", nodeClaim.Status.ProviderID, nodeClaim.Name, err)
			}
		}
	} else if machineAnno, ok := nodeClaim.Annotations[machineAnnotation]; ok {
		machineNamespace, machineName, err := parseMachineAnnotation(machineAnno)
		if err != nil {
			return fmt.Errorf("error parsing machine annotation: %w", err)
		}

		machine, err = c.machineProvider.Get(ctx, machineName, machineNamespace)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Machine is already gone. Decrement the MD replicas using the MD annotation so
				// the replica count stays accurate, then signal to karpenter that the NodeClaim
				// is no longer backed by a real machine.
				if mdAnno, ok := nodeClaim.Annotations[machineDeploymentAnnotation]; ok {
					if mdNamespace, mdName, parseErr := parseMachineAnnotation(mdAnno); parseErr == nil {
						c.tryDecrementMachineDeploymentReplicas(ctx, mdNamespace, mdName)
					}
				}
				return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("Machine %q in namespace %q no longer exists for NodeClaim %q", machineName, machineNamespace, nodeClaim.Name))
			}
			return fmt.Errorf("error finding Machine %q in namespace %s to Delete NodeClaim %q: %w", machineName, machineNamespace, nodeClaim.Name, err)
		}
	} else {
		return fmt.Errorf("NodeClaim %q does not have a provider ID or Machine annotations, cannot delete", nodeClaim.Name)
	}

	if machine == nil {
		// Machine was not found by ProviderID. This happens when the infrastructure provider
		// assigned a real ProviderID to the Machine (replacing our synthetic capi-pending:// one),
		// and then the Machine was later deleted (e.g. due to a provisioning failure with no
		// available servers). The replica count was incremented when the Machine was first
		// provisioned and must be decremented now; otherwise every retry by Karpenter will
		// increment from the already-elevated value, causing unbounded replica escalation.
		if mdAnno, ok := nodeClaim.Annotations[machineDeploymentAnnotation]; ok {
			if mdNamespace, mdName, parseErr := parseMachineAnnotation(mdAnno); parseErr == nil {
				c.tryDecrementMachineDeploymentReplicas(ctx, mdNamespace, mdName)
			}
		}
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("unable to find Machine with provider ID %q to Delete NodeClaim %q", nodeClaim.Status.ProviderID, nodeClaim.Name))
	}

	// check if already deleting
	if c.machineProvider.IsDeleting(machine) {
		// Machine is already being deleted — either by a previous Delete call that added the
		// delete annotation, or by an external CAPI scale-down.
		//
		// In both cases the replica decrement has already happened:
		//   - karpenter-initiated: we decremented in the first Delete call (not-IsDeleting path below).
		//   - CAPI-initiated scale-down: CAPI decremented MD.Spec.Replicas before picking the Machine.
		//
		// Do NOT decrement again. Return NodeClaimNotFoundError to tell karpenter's EnsureTerminated
		// that this NodeClaim is fully handled, so it removes the finalizer immediately (no requeue).
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("Machine %q for NodeClaim %q is already being deleted", machine.Name, nodeClaim.Name))
	}

	// mark the machine for deletion before decrementing replicas to protect against the wrong machine being removed
	if err := c.machineProvider.AddDeleteAnnotation(ctx, machine); err != nil {
		return fmt.Errorf("unable to delete NodeClaim %q, cannot annotate Machine %q for deletion: %w", nodeClaim.Name, machine.Name, err)
	}

	// Decrement MachineDeployment replicas with RetryOnConflict so that a stale cached
	// ResourceVersion (409 Conflict) does not permanently prevent the decrement.
	if retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		machineDeployment, err := c.machineDeploymentFromMachine(ctx, machine)
		if err != nil {
			return fmt.Errorf("cannot find owner MachineDeployment for Machine %q: %w", machine.Name, err)
		}
		if machineDeployment.Spec.Replicas == nil {
			return fmt.Errorf("MachineDeployment %q has nil replicas", machineDeployment.Name)
		}
		if *machineDeployment.Spec.Replicas == 0 {
			return fmt.Errorf("MachineDeployment %q is already at zero replicas", machineDeployment.Name)
		}
		machineDeployment.Spec.Replicas = ptr.To(*machineDeployment.Spec.Replicas - 1)
		return c.machineDeploymentProvider.Update(ctx, machineDeployment)
	}); retryErr != nil {
		// Cleanup the machine delete annotation so we don't affect future replica changes.
		if removeErr := c.machineProvider.RemoveDeleteAnnotation(ctx, machine); removeErr != nil {
			return fmt.Errorf("unable to delete NodeClaim %q, cannot remove delete annotation for Machine %q during cleanup: %w", nodeClaim.Name, machine.Name, removeErr)
		}
		return fmt.Errorf("unable to delete NodeClaim %q, cannot update MachineDeployment replicas: %w", nodeClaim.Name, retryErr)
	}

	// Return NodeClaimNotFoundError to signal to karpenter's EnsureTerminated that this NodeClaim
	// is fully handled. This causes it to return isTerminated=true, so finalize() removes the
	// NodeClaim finalizer immediately without requeueing.
	//
	// Returning nil here would cause a 5-second requeue. On that requeue, the Machine would
	// appear as IsDeleting (because we just added the annotation above), and the IsDeleting path
	// would be entered again — which could race with a new NodeClaim's Create incrementing replicas,
	// leading to a second (erroneous) decrement and replica escalation.
	return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("Machine %q for NodeClaim %q has been marked for deletion", machine.Name, nodeClaim.Name))
}

// Get returns a NodeClaim for the Machine object with the supplied provider ID, or nil if not found.
func (c *CloudProvider) Get(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	if len(providerID) == 0 {
		return nil, fmt.Errorf("no providerID supplied to Get, cannot continue")
	}

	var machine *capiv1beta1.Machine
	var err error

	if strings.HasPrefix(providerID, pendingProviderIDPrefix) {
		// Synthetic ProviderID: look up Machine directly by namespace/name.
		ref := strings.TrimPrefix(providerID, pendingProviderIDPrefix)
		machineNamespace, machineName, parseErr := parseMachineAnnotation(ref)
		if parseErr != nil {
			return nil, fmt.Errorf("error parsing pending provider ID %q: %w", providerID, parseErr)
		}
		machine, err = c.machineProvider.Get(ctx, machineName, machineNamespace)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("cannot find Machine %q/%q", machineNamespace, machineName))
			}
			return nil, fmt.Errorf("error getting Machine: %w", err)
		}
	} else {
		machine, err = c.machineProvider.GetByProviderID(ctx, providerID)
		if err != nil {
			return nil, fmt.Errorf("error getting Machine: %w", err)
		}
		if machine == nil {
			return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("cannot find Machine with provider ID %q", providerID))
		}
	}

	nodeClaim, err := c.machineToNodeClaim(ctx, machine)
	if err != nil {
		return nil, fmt.Errorf("unable to convert Machine to NodeClaim in CloudProvider.Get: %w", err)
	}

	return nodeClaim, nil
}

// GetInstanceTypes enumerates the known Cluster API scalable resources to generate the list
// of possible instance types.
func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *karpv1.NodePool) ([]*cloudprovider.InstanceType, error) {

	if nodePool == nil {
		return nil, fmt.Errorf("node pool reference is nil, no way to proceed")
	}

	nodeClass, err := c.resolveNodeClassFromNodePool(ctx, nodePool)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve NodeClass from NodePool %s: %w", nodePool.Name, err)
	}

	capiInstanceTypes, err := c.findInstanceTypesForNodeClass(ctx, nodeClass)
	if err != nil {
		return nil, fmt.Errorf("unable to get instance types for NodePool %q: %w", nodePool.Name, err)
	}

	instanceTypes := lo.Map(capiInstanceTypes, func(i *ClusterAPIInstanceType, _ int) *cloudprovider.InstanceType {
		return &cloudprovider.InstanceType{
			Name:         i.Name,
			Requirements: i.Requirements,
			Offerings:    i.Offerings,
			Capacity:     i.Capacity,
			Overhead:     i.Overhead,
		}
	})
	return instanceTypes, nil
}

func (c *CloudProvider) GetSupportedNodeClasses() []status.Object {
	return []status.Object{&v1alpha1.ClusterAPINodeClass{}}
}

// Return nothing since there's no cloud provider drift.
func (c *CloudProvider) IsDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim) (cloudprovider.DriftReason, error) {
	return "", nil
}

func (c *CloudProvider) List(ctx context.Context) ([]*karpv1.NodeClaim, error) {
	// select all machines that have the nodepool membership label, this should be all the machines that are registered as nodes
	selector := metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      providers.NodePoolMemberLabel,
				Operator: metav1.LabelSelectorOpExists,
			},
		},
	}
	machines, err := c.machineProvider.List(ctx, &selector)
	if err != nil {
		return nil, fmt.Errorf("listing machines, %w", err)
	}

	var nodeClaims []*karpv1.NodeClaim
	for _, machine := range machines {
		nodeClaim, err := c.machineToNodeClaim(ctx, machine)
		if err != nil {
			return []*karpv1.NodeClaim{}, fmt.Errorf("unable to convert Machine %s to NodeClaim: %w", machine.Name, err)
		}
		nodeClaims = append(nodeClaims, nodeClaim)
	}

	return nodeClaims, nil
}

func (c *CloudProvider) Name() string {
	return "clusterapi"
}

func (c *CloudProvider) RepairPolicies() []cloudprovider.RepairPolicy {
	// TODO(elmiko) research what this means for cluster-api, perhaps there are conditions that
	// we could use from cluster-api to determine when repair should be initiated.

	return []cloudprovider.RepairPolicy{}
}

// ProvisionMachine ensures a CAPI Machine exists for the given NodeClaim.
// It creates the Machine if missing, or "gets" it to confirm the asynchronous provisioning status.
func (c *CloudProvider) provisionMachine(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*capiv1beta1.MachineDeployment, *capiv1beta1.Machine, error) {
	machineAnno, ok := nodeClaim.Annotations[machineAnnotation]
	if !ok {
		return c.createMachine(ctx, nodeClaim)
	}

	machineNamespace, machineName, err := parseMachineAnnotation(machineAnno)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing machine annotation: %w", err)
	}

	machine, err := c.machineProvider.Get(ctx, machineName, machineNamespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The Machine no longer exists. The previous createMachine call already incremented
			// the MachineDeployment replicas for a Machine that is now gone. Decrement that
			// phantom replica before starting a fresh provisioning attempt to prevent escalation.
			if mdAnno, ok := nodeClaim.Annotations[machineDeploymentAnnotation]; ok {
				if mdNamespace, mdName, parseErr := parseMachineAnnotation(mdAnno); parseErr == nil {
					c.tryDecrementMachineDeploymentReplicas(ctx, mdNamespace, mdName)
				}
			}
			return c.createMachine(ctx, nodeClaim)
		}
		return nil, nil, fmt.Errorf("failed to get NodeClaim's Machine %s : %w", machineName, err)
	}

	machineDeployment, err := c.machineDeploymentFromMachine(ctx, machine)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get NodeClaim's MachineDeployment %s : %w", machineName, err)
	}

	return machineDeployment, machine, nil
}

func (c *CloudProvider) createMachine(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*capiv1beta1.MachineDeployment, *capiv1beta1.Machine, error) {
	nodeClass, err := c.resolveNodeClassFromNodeClaim(ctx, nodeClaim)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to resolve NodeClass from NodeClaim %q: %w", nodeClaim.Name, err)
	}

	instanceTypes, err := c.findInstanceTypesForNodeClass(ctx, nodeClass)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to get instance types for NodeClass %q of NodeClaim %q: %w", nodeClass.Name, nodeClaim.Name, err)
	}

	// identify which fit requirements
	compatibleInstanceTypes := filterCompatibleInstanceTypes(instanceTypes, nodeClaim)
	if len(compatibleInstanceTypes) == 0 {
		return nil, nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("cannot satisfy create, no compatible instance types found"))
	}

	// TODO (elmiko) if multiple instance types are found to be compatible we need to select one.
	// for now, we sort by resource name and take the first in the list. In the future, this should
	// be an option or something more useful like minimum size or cost.
	slices.SortFunc(compatibleInstanceTypes, func(a, b *ClusterAPIInstanceType) int {
		return cmp.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name))
	})
	selectedInstanceType := compatibleInstanceTypes[0]

	// once scalable resource is identified, increase replicas
	machineDeployment, err := c.machineDeploymentProvider.Get(ctx, selectedInstanceType.MachineDeploymentName, selectedInstanceType.MachineDeploymentNamespace)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to find MachineDeployment %q for InstanceType %q: %w", selectedInstanceType.MachineDeploymentName, selectedInstanceType.Name, err)
	}

	rollbackCtx := context.WithoutCancel(ctx)

	// Before incrementing replicas, check if a prior failed attempt already allocated an
	// unclaimed Machine for this MachineDeployment. Reusing it avoids inflating the replica
	// count on each retry cycle, which is the root cause of replica escalation.
	// Important: if an existing Machine is found but claiming it fails, we must return an error
	// rather than fall through to the increment path — incrementing when MD is already elevated
	// by a phantom replica would over-count replicas by one.
	if existingMachine := c.findUnclaimedMachineForMachineDeployment(ctx, machineDeployment); existingMachine != nil {
		return c.claimMachine(ctx, rollbackCtx, machineDeployment, existingMachine, nodeClaim)
	}

	originalReplicas := *machineDeployment.Spec.Replicas
	machineDeployment.Spec.Replicas = ptr.To(originalReplicas + 1)
	if err := c.machineDeploymentProvider.Update(ctx, machineDeployment); err != nil {
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to update MachineDeployment %q replicas: %w", machineDeployment.Name, err)
	}

	// rollbackReplicas resets the MachineDeployment replica count to originalReplicas.
	// context.WithoutCancel preserves tracing/logging values from ctx while detaching its cancellation,
	// so the rollback succeeds even if the outer ctx has already expired.
	// We use machineDeployment directly on the first attempt since it already carries the fresh
	// ResourceVersion from the successful increment update, avoiding a stale informer cache read.
	// Only on conflict do we re-fetch to get the latest ResourceVersion before retrying.
	rollbackReplicas := func() {
		md := machineDeployment
		if retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			md.Spec.Replicas = ptr.To(originalReplicas)
			if err := c.machineDeploymentProvider.Update(rollbackCtx, md); err != nil {
				if latest, getErr := c.machineDeploymentProvider.Get(rollbackCtx, selectedInstanceType.MachineDeploymentName, selectedInstanceType.MachineDeploymentNamespace); getErr == nil {
					md = latest
				}
				return err
			}
			return nil
		}); retryErr != nil {
			log.Println(fmt.Errorf("error while recovering replicas for MachineDeployment %q for InstanceType %q: %w", selectedInstanceType.MachineDeploymentName, selectedInstanceType.Name, retryErr))
		}
	}

	// TODO (elmiko) it would be nice to have a more elegant solution to the asynchronous machine creation.
	// Initially, it appeared that we could have a Machine controller which could reconcile new Machines and
	// then associate them with NodeClaims by using a sentinel value for the Provider ID. But, this may not
	// work as we expect since the karpenter core can use the Provider ID as a key into one of its internal caches.
	// For now, the method of waiting for the Machine seemed straightforward although it does make the `Create` method a blocking call.
	// Try to find an unclaimed Machine resource for 1 minute.
	machine, err := c.pollForUnclaimedMachineInMachineDeploymentWithTimeout(ctx, machineDeployment, time.Minute)
	if err != nil {
		// unable to find a Machine for the NodeClaim, this could be due to timeout or error, but the replica count needs to be reset.
		rollbackReplicas()
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to find an unclaimed Machine for MachineDeployment %q: %w", machineDeployment.Name, err)
	}

	md, m, claimErr := c.claimMachine(ctx, rollbackCtx, machineDeployment, machine, nodeClaim)
	if claimErr != nil {
		rollbackReplicas()
		return nil, nil, claimErr
	}

	return md, m, nil
}

// findUnclaimedMachineForMachineDeployment returns the first non-deleting Machine in the
// MachineDeployment that has not yet been claimed by karpenter, or nil if none exist.
func (c *CloudProvider) findUnclaimedMachineForMachineDeployment(ctx context.Context, machineDeployment *capiv1beta1.MachineDeployment) *capiv1beta1.Machine {
	selector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      providers.NodePoolMemberLabel,
				Operator: metav1.LabelSelectorOpDoesNotExist,
			},
			{
				Key:      capiv1beta1.MachineDeploymentNameLabel,
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{machineDeployment.Name},
			},
		},
	}
	machineList, err := c.machineProvider.List(ctx, selector)
	if err != nil {
		return nil
	}
	for _, m := range machineList {
		if m.GetDeletionTimestamp().IsZero() {
			return m
		}
	}
	return nil
}

// claimMachine labels machine as a karpenter member and binds it to nodeClaim via annotations.
// rollbackCtx is used for cleanup operations that must survive cancellation of ctx.
func (c *CloudProvider) claimMachine(ctx, rollbackCtx context.Context, machineDeployment *capiv1beta1.MachineDeployment, machine *capiv1beta1.Machine, nodeClaim *karpv1.NodeClaim) (*capiv1beta1.MachineDeployment, *capiv1beta1.Machine, error) {
	labels := machine.GetLabels()
	labels[providers.NodePoolMemberLabel] = ""
	machine.SetLabels(labels)
	if err := c.machineProvider.Update(ctx, machine); err != nil {
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to label Machine %q as a member: %w", machine.Name, err)
	}

	// Bind the NodeClaim with this machine and its owning MachineDeployment.
	// The MD annotation is used to decrement replicas if the Machine disappears before
	// the NodeClaim is deleted, preventing replica escalation on re-provision.
	nodeClaim.Annotations[machineAnnotation] = fmt.Sprintf("%s/%s", machine.Namespace, machine.Name)
	nodeClaim.Annotations[machineDeploymentAnnotation] = fmt.Sprintf("%s/%s", machineDeployment.Namespace, machineDeployment.Name)
	if err := c.kubeClient.Update(ctx, nodeClaim); err != nil {
		// The Machine was already labeled as a member; remove that label before rolling back replicas
		// so future polls can still find unclaimed Machines correctly.
		// RetryOnConflict handles potential 409 Conflicts from a stale cached ResourceVersion.
		if unlabelErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			m, getErr := c.machineProvider.Get(rollbackCtx, machine.Name, machine.Namespace)
			if getErr != nil {
				return getErr
			}
			delete(m.Labels, providers.NodePoolMemberLabel)
			return c.machineProvider.Update(rollbackCtx, m)
		}); unlabelErr != nil {
			log.Println(fmt.Errorf("cannot satisfy create, unable to remove member label from Machine %q during cleanup: %w", machine.Name, unlabelErr))
		}
		return nil, nil, fmt.Errorf("cannot satisfy create, unable to update NodeClaim annotations %q: %w", nodeClaim.Name, err)
	}

	return machineDeployment, machine, nil
}

// tryDecrementMachineDeploymentReplicas decrements the replica count of the named MachineDeployment
// by one. It is a best-effort operation: errors are logged but not returned, because it is used in
// cleanup paths where the caller cannot meaningfully handle a failure.
func (c *CloudProvider) tryDecrementMachineDeploymentReplicas(ctx context.Context, mdNamespace, mdName string) {
	cleanupCtx := context.WithoutCancel(ctx)
	if retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		md, err := c.machineDeploymentProvider.Get(cleanupCtx, mdName, mdNamespace)
		if err != nil {
			return err
		}
		if md.Spec.Replicas != nil && *md.Spec.Replicas > 0 {
			md.Spec.Replicas = ptr.To(*md.Spec.Replicas - 1)
			return c.machineDeploymentProvider.Update(cleanupCtx, md)
		}
		return nil
	}); retryErr != nil {
		log.Println(fmt.Errorf("error decrementing replicas for MachineDeployment %q/%q: %w", mdNamespace, mdName, retryErr))
	}
}

func (c *CloudProvider) machineDeploymentFromMachine(ctx context.Context, machine *capiv1beta1.Machine) (*capiv1beta1.MachineDeployment, error) {
	mdName, found := machine.GetLabels()[capiv1beta1.MachineDeploymentNameLabel]
	if !found {
		return nil, fmt.Errorf("unable to find MachineDeployment for Machine %q, has no MachineDeployment label %q", machine.GetName(), capiv1beta1.MachineDeploymentNameLabel)
	}

	machineDeployment, err := c.machineDeploymentProvider.Get(ctx, mdName, machine.GetNamespace())
	if err != nil {
		return nil, fmt.Errorf("unable to get MachineDeployment %s for Machine %s: %w", mdName, machine.GetName(), err)
	}

	return machineDeployment, nil
}

func (c *CloudProvider) findInstanceTypesForNodeClass(ctx context.Context, nodeClass *v1alpha1.ClusterAPINodeClass) ([]*ClusterAPIInstanceType, error) {
	instanceTypes := []*ClusterAPIInstanceType{}

	if nodeClass == nil {
		return instanceTypes, fmt.Errorf("unable to find instance types for nil NodeClass")
	}

	machineDeployments, err := c.machineDeploymentProvider.List(ctx, nodeClass.Spec.ScalableResourceSelector)
	if err != nil {
		return instanceTypes, fmt.Errorf("unable to list MachineDeployments for NodeClass %s: %w", nodeClass.Name, err)
	}

	for _, md := range machineDeployments {
		it := machineDeploymentToInstanceType(md)
		instanceTypes = append(instanceTypes, it)
	}

	return instanceTypes, nil
}

func (c *CloudProvider) machineToNodeClaim(ctx context.Context, machine *capiv1beta1.Machine) (*karpv1.NodeClaim, error) {
	nodeClaim := karpv1.NodeClaim{}
	if machine.Spec.ProviderID != nil {
		nodeClaim.Status.ProviderID = *machine.Spec.ProviderID
	}

	// we want to get the MachineDeployment that owns this Machine to read the capacity information.
	// to being this process, we get the MachineDeployment name from the Machine labels.
	machineDeployment, err := c.machineDeploymentFromMachine(ctx, machine)
	if err != nil {
		return nil, fmt.Errorf("unable to convert Machine %q to a NodeClaim, cannot find MachineDeployment: %w", machine.Name, err)
	}

	// machine capacity
	// we are using the scale from zero annotations on the MachineDeployment to make this accessible.
	// TODO (elmiko) improve this once upstream has advanced the state of the art for getting capacity,
	// also add a mechanism to lookup the infra machine template similar to how CAS does it.
	capacity := capacityResourceListFromAnnotations(machineDeployment.GetAnnotations())
	_, found := capacity[corev1.ResourceCPU]
	if !found {
		// if there is no cpu resource we aren't going to get far, return an error
		return nil, fmt.Errorf("unable to convert Machine %q to a NodeClaim, no cpu capacity found on MachineDeployment %q", machine.GetName(), machineDeployment.Name)
	}

	_, found = capacity[corev1.ResourceMemory]
	if !found {
		// if there is no memory resource we aren't going to get far, return an error
		return nil, fmt.Errorf("unable to convert Machine %q to a NodeClaim, no memory capacity found on MachineDeployment %q", machine.GetName(), machineDeployment.Name)
	}

	// Set NodeClaim labels from the MachineDeployment
	nodeClaim.Labels = nodeLabelsFromMachineDeployment(machineDeployment)

	// TODO (elmiko) add taints

	nodeClaim.Status.Capacity = capacity

	return &nodeClaim, nil
}

func (c *CloudProvider) pollForUnclaimedMachineInMachineDeploymentWithTimeout(ctx context.Context, machineDeployment *capiv1beta1.MachineDeployment, timeout time.Duration) (*capiv1beta1.Machine, error) {
	var machine *capiv1beta1.Machine

	// select all Machines that have the ownership label for the MachineDeployment, and do not have the
	// label for karpenter node pool membership.
	selector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      providers.NodePoolMemberLabel,
				Operator: metav1.LabelSelectorOpDoesNotExist,
			},
			{
				Key:      capiv1beta1.MachineDeploymentNameLabel,
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{machineDeployment.Name},
			},
		},
	}

	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		machineList, err := c.machineProvider.List(ctx, selector)
		if err != nil {
			// this might need to ignore the error for the sake of the timeout
			return false, fmt.Errorf("error listing unclaimed Machines for MachineDeployment %q: %w", machineDeployment.Name, err)
		}
		for _, m := range machineList {
			// Skip Machines that are already being deleted; claiming one would bind the NodeClaim
			// to a Machine that will never become a Node, and the subsequent Delete call would
			// not reduce the replica count (IsDeleting guard), leaving MD permanently elevated.
			if m.GetDeletionTimestamp().IsZero() {
				machine = m
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("error polling for an unclaimed Machine in MachineDeployment %q: %w", machineDeployment.Name, err)
	}

	return machine, nil
}

func (c *CloudProvider) resolveNodeClassFromNodeClaim(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*v1alpha1.ClusterAPINodeClass, error) {
	nodeClass := &v1alpha1.ClusterAPINodeClass{}

	if nodeClaim == nil {
		return nil, fmt.Errorf("NodeClaim is nil, cannot resolve NodeClass")
	}

	if nodeClaim.Spec.NodeClassRef == nil {
		return nil, fmt.Errorf("NodeClass reference is nil for NodeClaim %q, cannot resolve NodeClass", nodeClaim.Name)
	}

	name := nodeClaim.Spec.NodeClassRef.Name
	if name == "" {
		return nil, fmt.Errorf("NodeClass reference name is empty for NodeClaim %q, cannot resolve NodeClass", nodeClaim.Name)
	}

	// TODO (elmiko) add extra logic to get different resources from the class ref
	// if the kind and version differ from the included api then we will need to load differently.
	// NodeClass and NodeClaim are not namespace scoped, this call can probably be changed.
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: name, Namespace: nodeClaim.Namespace}, nodeClass); err != nil {
		return nil, fmt.Errorf("unable to get NodeClass %s for NodeClaim %s: %w", name, nodeClaim.Name, err)
	}

	return nodeClass, nil
}

func (c *CloudProvider) resolveNodeClassFromNodePool(ctx context.Context, nodePool *karpv1.NodePool) (*v1alpha1.ClusterAPINodeClass, error) {
	nodeClass := &v1alpha1.ClusterAPINodeClass{}

	if nodePool == nil {
		return nil, fmt.Errorf("NodePool is nil, cannot resolve NodeClass")
	}

	if nodePool.Spec.Template.Spec.NodeClassRef == nil {
		return nil, fmt.Errorf("node class reference is nil, no way to proceed")
	}

	name := nodePool.Spec.Template.Spec.NodeClassRef.Name
	if name == "" {
		return nil, fmt.Errorf("node class reference name is empty, no way to proceed")
	}

	// TODO (elmiko) add extra logic to get different resources from the class ref
	// if the kind and version differ from the included api then we will need to load differently.
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: name, Namespace: nodePool.Namespace}, nodeClass); err != nil {
		return nil, fmt.Errorf("unable to get NodeClass %s for NodePool %s: %w", name, nodePool.Name, err)
	}

	return nodeClass, nil
}

func capacityResourceListFromAnnotations(annotations map[string]string) corev1.ResourceList {
	capacity := corev1.ResourceList{}

	if annotations == nil {
		return capacity
	}

	cpu, found := annotations[cpuKey]
	if found {
		capacity[corev1.ResourceCPU] = resource.MustParse(cpu)
	}

	memory, found := annotations[memoryKey]
	if found {
		capacity[corev1.ResourceMemory] = resource.MustParse(memory)
	}

	gpuCount, found := annotations[gpuCountKey]
	if found {
		// if there is a count there must also be a type
		gpuType, found := annotations[gpuTypeKey]
		if found {
			capacity[corev1.ResourceName(gpuType)] = resource.MustParse(gpuCount)
		}
	}

	ephStorage, found := annotations[diskCapacityKey]
	if found {
		capacity[corev1.ResourceEphemeralStorage] = resource.MustParse(ephStorage)
	}

	maxPods, found := annotations[maxPodsKey]
	if found {
		capacity[corev1.ResourcePods] = resource.MustParse(maxPods)
	}

	return capacity
}

func createNodeClaimFromMachineDeployment(machineDeployment *capiv1beta1.MachineDeployment) *karpv1.NodeClaim {
	nodeClaim := &karpv1.NodeClaim{}

	instanceType := machineDeploymentToInstanceType(machineDeployment)
	nodeClaim.Status.Capacity = instanceType.Capacity
	nodeClaim.Status.Allocatable = instanceType.Allocatable()

	// Set NodeClaim labels from the MachineDeployment
	nodeClaim.Labels = nodeLabelsFromMachineDeployment(machineDeployment)

	// TODO (elmiko) add taints

	return nodeClaim
}

func filterCompatibleInstanceTypes(instanceTypes []*ClusterAPIInstanceType, nodeClaim *karpv1.NodeClaim) []*ClusterAPIInstanceType {
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	filteredInstances := lo.Filter(instanceTypes, func(i *ClusterAPIInstanceType, _ int) bool {
		// TODO (elmiko) if/when we have offering availability, this is a good place to filter out unavailable instance types
		return reqs.Compatible(i.Requirements, scheduling.AllowUndefinedWellKnownLabels) == nil &&
			resources.Fits(nodeClaim.Spec.Resources.Requests, i.Allocatable())
	})

	return filteredInstances
}

func labelsFromScaleFromZeroAnnotation(annotation string) map[string]string {
	labels := map[string]string{}

	labelStrings := strings.Split(annotation, ",")
	for _, label := range labelStrings {
		split := strings.SplitN(label, "=", 2)
		if len(split) == 2 {
			labels[split[0]] = split[1]
		}
	}

	return labels
}

func machineDeploymentToInstanceType(machineDeployment *capiv1beta1.MachineDeployment) *ClusterAPIInstanceType {
	instanceType := &ClusterAPIInstanceType{}

	labels := nodeLabelsFromMachineDeployment(machineDeployment)
	requirements := []*scheduling.Requirement{}
	for k, v := range labels {
		requirements = append(requirements, scheduling.NewRequirement(k, corev1.NodeSelectorOpIn, v))
	}
	instanceType.Requirements = scheduling.NewRequirements(requirements...)

	capacity := capacityResourceListFromAnnotations(machineDeployment.GetAnnotations())
	instanceType.Capacity = capacity

	// TODO (elmiko) add offerings info, TBD of where this would come from
	// start with zone, read from the label and add to offering
	// initial price is 0
	// there is a single offering, and it is available
	zone := zoneLabelFromLabels(labels)
	requirements = []*scheduling.Requirement{
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
		scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
	}
	offerings := cloudprovider.Offerings{
		&cloudprovider.Offering{
			Requirements: scheduling.NewRequirements(requirements...),
			Price:        0.0,
			Available:    true,
		},
	}

	instanceType.Offerings = offerings

	// TODO (elmiko) find a better way to learn the instance type. The instance name needs to be the same
	// as the well-known `node.kubernetes.io/instance-type` that will be on resulting nodes. Usually, a
	// cloud controller, or similar, mechanism is used to apply this label.
	// For now, we check the labels we know about from the MachineDeployment, if the instance type label
	// is not there, leave it blank.
	// to the v1.LabelInstanceTypeStable. if karpenter expects this to match the node, then we need to get this value through capi.
	// TODO (jkyros) Add a test case to test this behavior
	instanceType.Name = labels[corev1.LabelInstanceTypeStable]

	// TODO (elmiko) add the proper overhead information, not sure where we will harvest this information.
	// perhaps it needs to be a configurable option somewhere.
	instanceType.Overhead = &cloudprovider.InstanceTypeOverhead{}

	// record the information from the MachineDeployment so we can find it again later.
	instanceType.MachineDeploymentName = machineDeployment.Name
	instanceType.MachineDeploymentNamespace = machineDeployment.Namespace

	return instanceType
}

func managedNodeLabelsFromLabels(labels map[string]string) map[string]string {
	managedLabels := map[string]string{}
	for key, value := range labels {
		dnsSubdomainOrName := strings.Split(key, "/")[0]
		if dnsSubdomainOrName == capiv1beta1.NodeRoleLabelPrefix {
			managedLabels[key] = value
		}
		if dnsSubdomainOrName == capiv1beta1.NodeRestrictionLabelDomain || strings.HasSuffix(dnsSubdomainOrName, "."+capiv1beta1.NodeRestrictionLabelDomain) {
			managedLabels[key] = value
		}
		if dnsSubdomainOrName == capiv1beta1.ManagedNodeLabelDomain || strings.HasSuffix(dnsSubdomainOrName, "."+capiv1beta1.ManagedNodeLabelDomain) {
			managedLabels[key] = value
		}
	}

	return managedLabels
}

func nodeLabelsFromMachineDeployment(machineDeployment *capiv1beta1.MachineDeployment) map[string]string {
	labels := map[string]string{}

	if machineDeployment.Spec.Template.Labels != nil {
		// get the labels that will be propagated to the node from the machinedeployment
		// see https://cluster-api.sigs.k8s.io/developer/architecture/controllers/metadata-propagation#metadata-propagation
		labels = managedNodeLabelsFromLabels(machineDeployment.Spec.Template.Labels)
	}

	// next we integrate labels from the scale-from-zero annotations, these can override
	// the propagated labels.
	// see https://github.com/kubernetes-sigs/cluster-api/blob/main/docs/proposals/20210310-opt-in-autoscaling-from-zero.md#machineset-and-machinedeployment-annotations
	if annotation, found := machineDeployment.GetAnnotations()[labelsKey]; found {
		for k, v := range labelsFromScaleFromZeroAnnotation(annotation) {
			labels[k] = v
		}
	}

	return labels
}

// zoneLabelFromLabels returns the value of the kubernetes well-known zone label or an empty string
func zoneLabelFromLabels(labels map[string]string) string {
	zone := ""

	if value, found := labels[corev1.LabelTopologyZone]; found {
		zone = value
	}

	return zone
}

func parseMachineAnnotation(annotationValue string) (string, string, error) {
	parts := strings.Split(annotationValue, "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid machine annotations '%s'. Expected 'namespace/name'", annotationValue)
	}

	ns := strings.TrimSpace(parts[0])
	name := strings.TrimSpace(parts[1])

	// Additional validation for empty strings
	if ns == "" || name == "" {
		return "", "", fmt.Errorf("invalid machine format '%s'. Namespace and name cannot be empty", annotationValue)
	}

	return ns, name, nil
}
