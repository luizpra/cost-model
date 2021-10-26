package kubecost

import (
	"fmt"
	"strconv"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/patrickmn/go-cache"
)

type ResourceTotals struct {
	Start   time.Time
	End     time.Time
	Cluster string
	Node    string
	CPUCost float64 `json:"cpu"`
	GPUCost float64 `json:"gpu"`
	RAMCost float64 `json:"ram"`
}

func ComputeResourceTotalsFromAllocations(as *AllocationSet, prop string) map[string]*ResourceTotals {
	rts := map[string]*ResourceTotals{}

	as.Each(func(name string, alloc *Allocation) {
		// Default to computing totals by Cluster, but allow override to use Node.
		key := alloc.Properties.Cluster
		if prop == AllocationNodeProp {
			key = alloc.Properties.Node
		}

		if rt, ok := rts[key]; ok {
			if rt.Start.After(alloc.Start) {
				rt.Start = alloc.Start
			}
			if rt.End.Before(alloc.End) {
				rt.End = alloc.End
			}

			if rt.Node != alloc.Properties.Node {
				rt.Node = ""
			}

			rt.CPUCost += alloc.CPUTotalCost()
			rt.RAMCost += alloc.RAMTotalCost()
			rt.GPUCost += alloc.GPUTotalCost()
		} else {
			rts[key] = &ResourceTotals{
				Start:   alloc.Start,
				End:     alloc.End,
				Cluster: alloc.Properties.Cluster,
				Node:    alloc.Properties.Node,
				CPUCost: alloc.CPUTotalCost(),
				RAMCost: alloc.RAMTotalCost(),
				GPUCost: alloc.GPUTotalCost(),
			}
		}
	})

	return rts
}

func ComputeResourceTotalsFromAssets(as *AssetSet, prop AssetProperty) map[string]*ResourceTotals {
	rts := map[string]*ResourceTotals{}

	as.Each(func(name string, asset Asset) {
		if node, ok := asset.(*Node); ok {
			// Default to computing totals by Cluster, but allow override to use Node.
			key := node.Properties().Cluster
			if prop == AssetNodeProp {
				key = node.Properties().Name
			}

			// adjustmentRate is used to scale resource costs proportionally
			// by the adjustment. This is necessary because we only get one
			// adjustment per Node, not one per-resource-per-Node.
			//
			// e.g. total cost = $90, adjustment = -$10 => 0.9
			// e.g. total cost = $150, adjustment = -$300 => 0.3333
			// e.g. total cost = $150, adjustment = $50 => 1.5
			adjustmentRate := 1.0
			if node.TotalCost()-node.Adjustment() == 0 {
				// If (totalCost - adjustment) is 0.0 then adjustment cancels
				// the entire node cost and we should make everything 0
				// without dividing by 0.
				adjustmentRate = 0.0
				log.DedupedWarningf(5, "ComputeResourceTotals: node cost adjusted to $0.00 for %s", node.Properties().Name)
			} else if node.Adjustment() != 0.0 {
				// adjustmentRate is the ratio of cost-with-adjustment (i.e. TotalCost)
				// to cost-without-adjustment (i.e. TotalCost - Adjustment).
				adjustmentRate = node.TotalCost() / (node.TotalCost() - node.Adjustment())
			}

			cpuCost := node.CPUCost * (1.0 - node.Discount) * adjustmentRate
			gpuCost := node.GPUCost * (1.0 - node.Discount) * adjustmentRate
			ramCost := node.RAMCost * (1.0 - node.Discount) * adjustmentRate

			// TODO should we add (cpu + gpu + ram)-(TotalCost) to one of them
			// to polish off any rounding errors?
			diff := cpuCost + gpuCost + ramCost - node.TotalCost()
			log.Infof("ComputeResourceTotals: unaccounted for %s $%.5f", node.Properties().Name, diff)

			if rt, ok := rts[key]; ok {
				if rt.Start.After(node.Start()) {
					rt.Start = node.Start()
				}
				if rt.End.Before(node.End()) {
					rt.End = node.End()
				}

				if rt.Node != node.Properties().Name {
					rt.Node = ""
				}

				rt.CPUCost += cpuCost
				rt.RAMCost += ramCost
				rt.GPUCost += gpuCost
			} else {
				rts[key] = &ResourceTotals{
					Start:   node.Start(),
					End:     node.End(),
					Cluster: node.Properties().Cluster,
					Node:    node.Properties().Name,
					CPUCost: cpuCost,
					RAMCost: ramCost,
					GPUCost: gpuCost,
				}
			}
		}
	})

	return rts
}

// ComputeIdleCoefficients returns the idle coefficients for CPU, GPU, and RAM
// (in that order) for the given resource costs and totals.
func ComputeIdleCoefficients(key string, cpuCost, gpuCost, ramCost float64, allocationTotals map[string]*ResourceTotals) (float64, float64, float64) {
	var cpuCoeff, gpuCoeff, ramCoeff float64

	if _, ok := allocationTotals[key]; !ok {
		return 0.0, 0.0, 0.0
	}

	if allocationTotals[key].CPUCost > 0 {
		cpuCoeff = cpuCost / allocationTotals[key].CPUCost
	}

	if allocationTotals[key].GPUCost > 0 {
		gpuCoeff = cpuCost / allocationTotals[key].GPUCost
	}

	if allocationTotals[key].RAMCost > 0 {
		ramCoeff = ramCost / allocationTotals[key].RAMCost
	}

	return cpuCoeff, gpuCoeff, ramCoeff
}

type ResourceTotalsStore interface {
	GetResourceTotalsByCluster(start, end time.Time) map[string]*ResourceTotals
	GetResourceTotalsByNode(start, end time.Time) map[string]*ResourceTotals
	SetResourceTotalsByCluster(start, end time.Time, rts map[string]*ResourceTotals)
	SetResourceTotalsByNode(start, end time.Time, rts map[string]*ResourceTotals)
}

type MemoryResourceTotalsStore struct {
	byCluster *cache.Cache
	byNode    *cache.Cache
}

func NewResourceTotalsStore() *MemoryResourceTotalsStore {
	return &MemoryResourceTotalsStore{
		byCluster: cache.New(cache.NoExpiration, cache.NoExpiration),
		byNode:    cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

func (mrts *MemoryResourceTotalsStore) GetResourceTotalsByCluster(start time.Time, end time.Time) map[string]*ResourceTotals {
	k := storeKey(start, end)
	if raw, ok := mrts.byCluster.Get(k); ok {
		return raw.(map[string]*ResourceTotals)
	} else {
		return nil
	}
}

func (mrts *MemoryResourceTotalsStore) GetResourceTotalsByNode(start time.Time, end time.Time) map[string]*ResourceTotals {
	k := storeKey(start, end)
	if raw, ok := mrts.byNode.Get(k); ok {
		return raw.(map[string]*ResourceTotals)
	} else {
		return nil
	}
}

func (mrts *MemoryResourceTotalsStore) SetResourceTotalsByCluster(start time.Time, end time.Time, rts map[string]*ResourceTotals) {
	k := storeKey(start, end)
	mrts.byCluster.Set(k, rts, cache.NoExpiration)
}

func (mrts *MemoryResourceTotalsStore) SetResourceTotalsByNode(start time.Time, end time.Time, rts map[string]*ResourceTotals) {
	k := storeKey(start, end)
	mrts.byNode.Set(k, rts, cache.NoExpiration)
}

// storeKey creates a storage key based on start and end times
func storeKey(start, end time.Time) string {
	startStr := strconv.FormatInt(start.Unix(), 10)
	endStr := strconv.FormatInt(end.Unix(), 10)
	return fmt.Sprintf("%s-%s", startStr, endStr)
}
