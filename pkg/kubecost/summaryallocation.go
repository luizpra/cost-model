package kubecost

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
)

// SummaryAllocation summarizes Allocation, keeping only the fields necessary
// for providing a high-level view of identifying the Allocation (Name) over a
// defined period of time (Start, End) and inspecting per-resource costs
// (subtotal with adjustment), total cost, and efficiency.
//
// TODO remove:
// Diff: 25 primitive 4 structs => 15 primitive, 1 struct
//
type SummaryAllocation struct {
	Name                   string                `json:"name"`
	Properties             *AllocationProperties `json:"-"`
	Start                  time.Time             `json:"start"`
	End                    time.Time             `json:"end"`
	CPUCoreRequestAverage  float64               `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage    float64               `json:"cpuCoreUsageAverage"`
	CPUCost                float64               `json:"cpuCost"`
	GPUCost                float64               `json:"gpuCost"`
	NetworkCost            float64               `json:"networkCost"`
	LoadBalancerCost       float64               `json:"loadBalancerCost"`
	PVCost                 float64               `json:"pvCost"`
	RAMBytesRequestAverage float64               `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage   float64               `json:"ramByteUsageAverage"`
	RAMCost                float64               `json:"ramCost"`
	SharedCost             float64               `json:"sharedCost"`
	ExternalCost           float64               `json:"externalCost"`
}

func NewSummaryAllocation(alloc *Allocation) *SummaryAllocation {
	if alloc == nil {
		return nil
	}

	// TODO evaluate performance penalties for "cloning" here

	return &SummaryAllocation{
		Name:                   alloc.Name,
		Properties:             alloc.Properties.Clone(), // TODO blerg
		Start:                  alloc.Start,
		End:                    alloc.End,
		CPUCoreRequestAverage:  alloc.CPUCoreRequestAverage,
		CPUCoreUsageAverage:    alloc.CPUCoreUsageAverage,
		CPUCost:                alloc.CPUCost + alloc.CPUCostAdjustment,
		GPUCost:                alloc.GPUCost + alloc.GPUCostAdjustment,
		NetworkCost:            alloc.NetworkCost + alloc.NetworkCostAdjustment,
		LoadBalancerCost:       alloc.LoadBalancerCost + alloc.LoadBalancerCostAdjustment,
		PVCost:                 alloc.PVCost(),
		RAMBytesRequestAverage: alloc.RAMBytesRequestAverage,
		RAMBytesUsageAverage:   alloc.RAMBytesUsageAverage,
		RAMCost:                alloc.RAMCost + alloc.RAMCostAdjustment,
		SharedCost:             alloc.SharedCost,
		ExternalCost:           alloc.ExternalCost,
	}
}

func (sa *SummaryAllocation) Add(that *SummaryAllocation, name string) {
	if sa == nil {
		log.Warningf("SummaryAllocation.Add: trying to add a nil receiver")
		return
	}

	// Overwrite the SummaryAllocation's name with the given name
	sa.Name = name

	// TODO do we need to maintain this?
	// Once Added, a SummaryAllocation has no Properties
	sa.Properties = nil

	// Sum non-cumulative fields by turning them into cumulative, adding them,
	// and then converting them back into averages after minutes have been
	// combined (just below).
	cpuReqCoreMins := sa.CPUCoreRequestAverage * sa.Minutes()
	cpuReqCoreMins += that.CPUCoreRequestAverage * that.Minutes()

	cpuUseCoreMins := sa.CPUCoreUsageAverage * sa.Minutes()
	cpuUseCoreMins += that.CPUCoreUsageAverage * that.Minutes()

	ramReqByteMins := sa.RAMBytesRequestAverage * sa.Minutes()
	ramReqByteMins += that.RAMBytesRequestAverage * that.Minutes()

	ramUseByteMins := sa.RAMBytesUsageAverage * sa.Minutes()
	ramUseByteMins += that.RAMBytesUsageAverage * that.Minutes()

	// Expand Start and End to be the "max" of among the given Allocations
	if that.Start.Before(sa.Start) {
		sa.Start = that.Start
	}
	if that.End.After(sa.End) {
		sa.End = that.End
	}

	// Convert cumulative request and usage back into rates
	if sa.Minutes() > 0 {
		sa.CPUCoreRequestAverage = cpuReqCoreMins / sa.Minutes()
		sa.CPUCoreUsageAverage = cpuUseCoreMins / sa.Minutes()
		sa.RAMBytesRequestAverage = ramReqByteMins / sa.Minutes()
		sa.RAMBytesUsageAverage = ramUseByteMins / sa.Minutes()
	} else {
		sa.CPUCoreRequestAverage = 0.0
		sa.CPUCoreUsageAverage = 0.0
		sa.RAMBytesRequestAverage = 0.0
		sa.RAMBytesUsageAverage = 0.0
	}

	// Sum all cumulative cost fields
	sa.CPUCost += that.CPUCost
	sa.ExternalCost += that.ExternalCost
	sa.GPUCost += that.GPUCost
	sa.LoadBalancerCost += that.LoadBalancerCost
	sa.NetworkCost += that.NetworkCost
	sa.PVCost += that.PVCost
	sa.RAMCost += that.RAMCost
	sa.SharedCost += that.SharedCost
}

// CPUEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (sa *SummaryAllocation) CPUEfficiency() float64 {
	if sa.CPUCoreRequestAverage > 0 {
		return sa.CPUCoreUsageAverage / sa.CPUCoreRequestAverage
	}

	if sa.CPUCoreUsageAverage == 0.0 || sa.CPUCost == 0.0 {
		return 0.0
	}

	return 1.0
}

func (sa *SummaryAllocation) generateKey(aggregateBy []string, labelConfig *LabelConfig) string {
	if sa == nil {
		return ""
	}

	return sa.Properties.GenerateKey(aggregateBy, labelConfig)
}

// IsExternal is true if the given Allocation represents external costs.
func (sa *SummaryAllocation) IsExternal() bool {
	return strings.Contains(sa.Name, ExternalSuffix)
}

// IsIdle is true if the given Allocation represents idle costs.
func (sa *SummaryAllocation) IsIdle() bool {
	return strings.Contains(sa.Name, IdleSuffix)
}

// IsUnallocated is true if the given Allocation represents unallocated costs.
func (sa *SummaryAllocation) IsUnallocated() bool {
	return strings.Contains(sa.Name, UnallocatedSuffix)
}

// Minutes returns the number of minutes the SummaryAllocation represents, as
// defined by the difference between the end and start times.
func (sa *SummaryAllocation) Minutes() float64 {
	return sa.End.Sub(sa.Start).Minutes()
}

// RAMEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (sa *SummaryAllocation) RAMEfficiency() float64 {
	if sa.RAMBytesRequestAverage > 0 {
		return sa.RAMBytesUsageAverage / sa.RAMBytesRequestAverage
	}

	if sa.RAMBytesUsageAverage == 0.0 || sa.RAMCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// TotalCost is the total cost of the SummaryAllocation
func (sa *SummaryAllocation) TotalCost() float64 {
	return sa.CPUCost + sa.GPUCost + sa.RAMCost + sa.PVCost + sa.NetworkCost + sa.LoadBalancerCost + sa.SharedCost + sa.ExternalCost
}

// TotalEfficiency is the cost-weighted average of CPU and RAM efficiency. If
// there is no cost at all, then efficiency is zero.
func (sa *SummaryAllocation) TotalEfficiency() float64 {
	if sa.RAMCost+sa.CPUCost > 0 {
		ramCostEff := sa.RAMEfficiency() * sa.RAMCost
		cpuCostEff := sa.CPUEfficiency() * sa.CPUCost
		return (ramCostEff + cpuCostEff) / (sa.CPUCost + sa.RAMCost)
	}

	return 0.0
}

// SummaryAllocationSet stores a set of SummaryAllocations, each with a unique
// name, that share a window. An AllocationSet is mutable, so treat it like a
// threadsafe map.
type SummaryAllocationSet struct {
	sync.RWMutex
	externalKeys       map[string]bool
	idleKeys           map[string]bool
	SummaryAllocations map[string]*SummaryAllocation `json:"allocations"`
	Window             Window                        `json:"window"`
}

func NewSummaryAllocationSet(as *AllocationSet) *SummaryAllocationSet {
	if as == nil {
		return nil
	}

	sas := &SummaryAllocationSet{
		SummaryAllocations: make(map[string]*SummaryAllocation, len(as.allocations)),
		Window:             as.Window.Clone(),
	}

	for _, alloc := range as.allocations {
		sas.SummaryAllocations[alloc.Name] = NewSummaryAllocation(alloc)
	}

	for key := range as.externalKeys {
		sas.externalKeys[key] = true
	}

	for key := range as.idleKeys {
		sas.idleKeys[key] = true
	}

	return sas
}

func (sas *SummaryAllocationSet) Add(that *SummaryAllocationSet) (*SummaryAllocationSet, error) {
	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return that, nil
	}

	if that == nil || len(that.SummaryAllocations) == 0 {
		return sas, nil
	}

	// Set start, end to min(start), max(end)
	start := *sas.Window.Start()
	end := *sas.Window.End()
	if that.Window.Start().Before(start) {
		start = *that.Window.Start()
	}
	if that.Window.End().After(end) {
		end = *that.Window.End()
	}

	acc := &SummaryAllocationSet{
		SummaryAllocations: make(map[string]*SummaryAllocation, len(sas.SummaryAllocations)),
		Window:             NewClosedWindow(start, end),
	}

	sas.RLock()
	defer sas.RUnlock()

	that.RLock()
	defer that.RUnlock()

	for _, alloc := range sas.SummaryAllocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	for _, alloc := range that.SummaryAllocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// AggregateBy aggregates the Allocations in the given AllocationSet by the given
// AllocationProperty. This will only be legal if the AllocationSet is divisible by the
// given AllocationProperty; e.g. Containers can be divided by Namespace, but not vice-a-versa.
func (sas *SummaryAllocationSet) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	// The order of operations for aggregating allocations is as follows:
	//
	//  1. Partition external, idle, and shared allocations into separate sets.
	//     Also, create the aggSet into which the results will be aggregated.
	//
	//  TODO (precompute)
	//  2. Compute sharing coefficients for idle and shared resources
	//     a) if idle allocation is to be shared, compute idle coefficients
	//     b) if idle allocation is NOT shared, but filters are present, compute
	//        idle filtration coefficients for the purpose of only returning the
	//        portion of idle allocation that would have been shared with the
	//        unfiltered results. (See unit tests 5.a,b,c)
	//     c) generate shared allocation for them given shared overhead, which
	//        must happen after (2a) and (2b)
	//     d) if there are shared resources, compute share coefficients
	//
	//  TODO (Move upstream to the StorageStrategy.Get() call; SQL?)
	//  3. Drop any allocation that fails any of the filters
	//
	//  TODO (from precomputed)
	//  4. Distribute idle allocations according to the idle coefficients
	//
	//  5. Generate aggregation key and insert allocation into the output set
	//
	//  TODO
	//  6. If idle is shared and resources are shared, some idle might be shared
	//     with a shared resource. Distribute that to the shared resources
	//     prior to sharing them with the aggregated results.
	//
	//  TODO
	//  7. Apply idle filtration coefficients from step (2b)
	//
	//  TODO
	//  8. Distribute shared allocations according to the share coefficients.
	//
	//  TODO
	//  9. If there are external allocations that can be aggregated into
	//     the output (i.e. they can be used to generate a valid key for
	//     the given properties) then aggregate; otherwise... ignore them?
	//
	//  TODO
	// 10. If the merge idle option is enabled, merge any remaining idle
	//     allocations into a single idle allocation. If there was any idle
	//	   whose costs were not distributed because there was no usage of a
	//     specific resource type, re-add the idle to the aggregation with
	//     only that type.
	//
	//  TODO
	// 11. Distribute any undistributed idle, in the case that idle
	//     coefficients end up being zero and some idle is not shared.

	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return nil
	}

	if options == nil {
		options = &AllocationAggregationOptions{}
	}

	if options.LabelConfig == nil {
		options.LabelConfig = NewLabelConfig()
	}

	// Check if we have any work to do; if not, then early return. If
	// aggregateBy is nil, we don't aggregate anything. On the other hand,
	// an empty slice implies that we should aggregate everything. (See
	// generateKey for why that makes sense.)
	shouldAggregate := aggregateBy != nil
	shouldFilter := len(options.FilterFuncs) > 0
	shouldShare := len(options.SharedHourlyCosts) > 0 || len(options.ShareFuncs) > 0
	if !shouldAggregate && !shouldFilter && !shouldShare {
		return nil
	}

	// aggSet will collect the aggregated allocations
	aggSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// externalSet will collect external allocations
	externalSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// idleSet will be shared among aggSet after initial aggregation
	// is complete
	idleSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	sas.Lock()
	defer sas.Unlock()

	// TODO
	log.Infof("SummaryAllocation: idle: %s (%d idle allocs)", options.ShareIdle, len(sas.idleKeys))

	// (1) Loop and find all of the external, idle, and shared allocations. Add
	// them to their respective sets, removing them from the set of allocations
	// to aggregate.
	for _, sa := range sas.SummaryAllocations {
		// External allocations get aggregated post-hoc (see step 6) and do
		// not necessarily contain complete sets of properties, so they are
		// moved to a separate AllocationSet.
		if sa.IsExternal() {
			delete(sas.externalKeys, sa.Name)
			delete(sas.SummaryAllocations, sa.Name)
			externalSet.Insert(sa)
			continue
		}

		// Idle allocations should be separated into idleSet if they are to be
		// shared later on. If they are not to be shared, then add them to the
		// aggSet like any other allocation.
		if sa.IsIdle() {
			delete(sas.idleKeys, sa.Name)
			delete(sas.SummaryAllocations, sa.Name)

			if options.ShareIdle == ShareEven || options.ShareIdle == ShareWeighted {
				idleSet.Insert(sa)
			} else {
				aggSet.Insert(sa)
			}

			continue
		}
	}

	// TODO do we need to handle case where len(SummaryAllocations) == 0?

	// (2) Idle and share coefficients (TODO)

	// (3) Filter (TODO, SQL)
	// (4) Distribute idle cost (TODO using precomputed)
	// (5) Aggregate
	for _, sa := range sas.SummaryAllocations {
		log.Infof("SummaryAllocation: %d idle allocations", len(idleSet.SummaryAllocations))

		// (4) Distribute idle allocations according to the idle coefficients
		// NOTE: if idle allocation is off (i.e. ShareIdle == ShareNone) then
		// all idle allocations will be in the aggSet at this point, so idleSet
		// will be empty and we won't enter this block.
		if len(idleSet.SummaryAllocations) > 0 {
			// Distribute idle allocations by coefficient per-idleId, per-allocation
			for _, idleAlloc := range idleSet.SummaryAllocations {
				var allocTotals map[string]*ResourceTotals
				var key string

				// Only share idleAlloc with current allocation (sa) if the
				// relevant property matches (i.e. Cluster or Node, depending
				// on which idle sharing option is selected)
				if options.IdleByNode {
					if idleAlloc.Properties.Node != sa.Properties.Node {
						continue
					}

					key = idleAlloc.Properties.Node

					allocTotals = options.AllocationResourceTotalsStore.GetResourceTotalsByNode(*sas.Window.Start(), *sas.Window.End())
					if allocTotals == nil {
						// TODO
						log.Warningf("SummaryAllocation: nil allocTotals by node for %s", sas.Window)
					}
				} else {
					if idleAlloc.Properties.Cluster != sa.Properties.Cluster {
						continue
					}

					key = idleAlloc.Properties.Cluster

					allocTotals = options.AllocationResourceTotalsStore.GetResourceTotalsByCluster(*sas.Window.Start(), *sas.Window.End())
					if allocTotals == nil {
						// TODO
						log.Warningf("SummaryAllocation: nil allocTotals by cluster for %s", sas.Window)
					}
				}

				cpuCoeff, gpuCoeff, ramCoeff := ComputeIdleCoefficients(key, sa.CPUCost, sa.GPUCost, sa.RAMCost, allocTotals)
				log.Infof("SummaryAllocation: idle coeffs for %s: %.3f, %.3f, %.3f", key, cpuCoeff, gpuCoeff, ramCoeff)

				sa.CPUCost += idleAlloc.CPUCost * cpuCoeff
				sa.GPUCost += idleAlloc.GPUCost * gpuCoeff
				sa.RAMCost += idleAlloc.RAMCost * ramCoeff
			}
		}

		// (5) generate key to use for aggregation-by-key and allocation name
		key := sa.generateKey(aggregateBy, options.LabelConfig)

		// The key becomes the allocation's name, which is used as the key by
		// which the allocation is inserted into the set.
		sa.Name = key

		// If merging unallocated allocations, rename all unallocated
		// allocations as simply __unallocated__
		if options.MergeUnallocated && sa.IsUnallocated() {
			sa.Name = UnallocatedSuffix
		}

		// Inserting the allocation with the generated key for a name will
		// perform the actual aggregation step.
		aggSet.Insert(sa)
	}

	// (6) Distribute idle to shared resources (TODO)

	// (7) Apply idle filtration (TODO)

	// (8) Distribute shared resources (TODO)

	// (9) External allocations

	// (10) Combine all idle allocations into a single "__idle__" allocation
	if !options.SplitIdle {
		for _, idleAlloc := range aggSet.IdleAllocations() {
			aggSet.Delete(idleAlloc.Name)
			idleAlloc.Name = IdleSuffix
			aggSet.Insert(idleAlloc)
		}
	}

	// (11 Distribute remaining, undistributed idle (TODO)

	// Replace the existing set's data with the new, aggregated summary data
	sas.SummaryAllocations = aggSet.SummaryAllocations

	return nil
}

func (sas *SummaryAllocationSet) InsertIdleSummaryAllocations(rts map[string]*ResourceTotals, prop AssetProperty) error {
	if sas == nil {
		return errors.New("cannot compute idle allocation for nil SummaryAllocationSet")
	}

	if len(rts) == 0 {
		return nil
	}

	// TODO argh avoid copy? Not the worst thing at this size... O(clusters) or O(nodes)
	idleTotals := make(map[string]*ResourceTotals, len(rts))
	for key, rt := range rts {
		idleTotals[key] = &ResourceTotals{
			Start:   rt.Start,
			End:     rt.End,
			CPUCost: rt.CPUCost,
			GPUCost: rt.GPUCost,
			RAMCost: rt.RAMCost,
		}
	}

	// Subtract allocated costs from resource totals, leaving only the remaining
	// idle totals for each key (cluster or node).
	sas.Each(func(name string, sa *SummaryAllocation) {
		key := sa.Properties.Cluster
		if prop == AssetNodeProp {
			key = sa.Properties.Node
		}

		if _, ok := idleTotals[key]; !ok {
			// Failed to find totals for the allocation's cluster or node.
			// (Should never happen.)
			log.Warningf("InsertIdleSummaryAllocations: failed to find %s: %s", prop, key)
			return
		}

		idleTotals[key].CPUCost -= sa.CPUCost
		idleTotals[key].GPUCost -= sa.GPUCost
		idleTotals[key].RAMCost -= sa.RAMCost
	})

	// Turn remaining idle totals into idle allocations and insert them.
	for key, rt := range idleTotals {
		idleAlloc := &SummaryAllocation{
			Name: fmt.Sprintf("%s/%s", key, IdleSuffix),
			Properties: &AllocationProperties{
				Cluster: rt.Cluster,
				Node:    rt.Node,
			},
			Start:   rt.Start,
			End:     rt.End,
			CPUCost: rt.CPUCost,
			GPUCost: rt.GPUCost,
			RAMCost: rt.RAMCost,
		}

		sas.Insert(idleAlloc)
	}

	return nil
}

// Delete removes the allocation with the given name from the set
func (sas *SummaryAllocationSet) Delete(name string) {
	if sas == nil {
		return
	}

	sas.Lock()
	defer sas.Unlock()

	delete(sas.externalKeys, name)
	delete(sas.idleKeys, name)
	delete(sas.SummaryAllocations, name)
}

// Each invokes the given function for each SummaryAllocation in the set
func (sas *SummaryAllocationSet) Each(f func(string, *SummaryAllocation)) {
	if sas == nil {
		return
	}

	for k, a := range sas.SummaryAllocations {
		f(k, a)
	}
}

// IdleAllocations returns a map of the idle allocations in the AllocationSet.
func (sas *SummaryAllocationSet) IdleAllocations() map[string]*SummaryAllocation {
	idles := map[string]*SummaryAllocation{}

	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return idles
	}

	sas.RLock()
	defer sas.RUnlock()

	for key := range sas.idleKeys {
		if sa, ok := sas.SummaryAllocations[key]; ok {
			idles[key] = sa // TODO Clone()?
		}
	}

	return idles
}

// Insert aggregates the current entry in the SummaryAllocationSet by the given Allocation,
// but only if the Allocation is valid, i.e. matches the SummaryAllocationSet's window. If
// there is no existing entry, one is created. Nil error response indicates success.
func (sas *SummaryAllocationSet) Insert(sa *SummaryAllocation) error {
	if sas == nil {
		return fmt.Errorf("cannot insert into nil SummaryAllocationSet")
	}

	sas.Lock()
	defer sas.Unlock()

	if sas.SummaryAllocations == nil {
		sas.SummaryAllocations = map[string]*SummaryAllocation{}
	}

	if sas.externalKeys == nil {
		sas.externalKeys = map[string]bool{}
	}

	if sas.idleKeys == nil {
		sas.idleKeys = map[string]bool{}
	}

	// Add the given Allocation to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := sas.SummaryAllocations[sa.Name]; ok {
		sas.SummaryAllocations[sa.Name].Add(sa, sa.Name)
	} else {
		sas.SummaryAllocations[sa.Name] = sa
	}

	// If the given Allocation is an external one, record that
	if sa.IsExternal() {
		sas.externalKeys[sa.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if sa.IsIdle() {
		sas.idleKeys[sa.Name] = true
	}

	return nil
}

// SummaryAllocationSetRange is a thread-safe slice of SummaryAllocationSets.
type SummaryAllocationSetRange struct {
	sync.RWMutex
	Step                  time.Duration           `json:"step"`
	SummaryAllocationSets []*SummaryAllocationSet `json:"sets"`
	Window                Window                  `json:"window"`
}

// NewSummaryAllocationSetRange instantiates a new range composed of the given
// SummaryAllocationSets in the order provided.
func NewSummaryAllocationSetRange(sass ...*SummaryAllocationSet) *SummaryAllocationSetRange {
	var step time.Duration
	window := NewWindow(nil, nil)

	for _, sas := range sass {
		if window.Start() == nil || (sas.Window.Start() != nil && sas.Window.Start().Before(*window.Start())) {
			window.start = sas.Window.Start()
		}
		if window.End() == nil || (sas.Window.End() != nil && sas.Window.End().After(*window.End())) {
			window.end = sas.Window.End()
		}
		if step == 0 {
			step = sas.Window.Duration()
		} else if step != sas.Window.Duration() {
			log.Warningf("instantiating range with step %s using set of step %s is illegal", step, sas.Window.Duration())
		}
	}

	return &SummaryAllocationSetRange{
		Step:                  step,
		SummaryAllocationSets: sass,
		Window:                window,
	}
}

// Accumulate sums each AllocationSet in the given range, returning a single cumulative
// AllocationSet for the entire range.
func (sasr *SummaryAllocationSetRange) Accumulate() (*SummaryAllocationSet, error) {
	var result *SummaryAllocationSet
	var err error

	sasr.RLock()
	defer sasr.RUnlock()

	for _, sas := range sasr.SummaryAllocationSets {
		result, err = result.Add(sas)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// AggregateBy aggregates each AllocationSet in the range by the given
// properties and options.
func (sasr *SummaryAllocationSetRange) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	sasr.Lock()
	defer sasr.Unlock()

	for _, sas := range sasr.SummaryAllocationSets {
		err := sas.AggregateBy(aggregateBy, options)
		if err != nil {
			return err
		}
	}

	return nil
}

// Append appends the given AllocationSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (sasr *SummaryAllocationSetRange) Append(sas *SummaryAllocationSet) error {
	if sasr.Step != 0 && sas.Window.Duration() != sasr.Step {
		return fmt.Errorf("cannot append set with duration %s to range of step %s", sas.Window.Duration(), sasr.Step)
	}

	sasr.Lock()
	defer sasr.Unlock()

	// Append to list of sets
	sasr.SummaryAllocationSets = append(sasr.SummaryAllocationSets, sas)

	// Set step, if not set
	if sasr.Step == 0 {
		sasr.Step = sas.Window.Duration()
	}

	// Adjust window
	if sasr.Window.Start() == nil || (sas.Window.Start() != nil && sas.Window.Start().Before(*sasr.Window.Start())) {
		sasr.Window.start = sas.Window.Start()
	}
	if sasr.Window.End() == nil || (sas.Window.End() != nil && sas.Window.End().After(*sasr.Window.End())) {
		sasr.Window.end = sas.Window.End()
	}

	return nil
}

// Each invokes the given function for each AllocationSet in the range
func (sasr *SummaryAllocationSetRange) Each(f func(int, *SummaryAllocationSet)) {
	if sasr == nil {
		return
	}

	for i, as := range sasr.SummaryAllocationSets {
		f(i, as)
	}
}

// TODO Custom MarshalJSON and UnmarshalJSON for these?
// - Step is uintelligible (microseconds??)
// - TotalCost would be nice
