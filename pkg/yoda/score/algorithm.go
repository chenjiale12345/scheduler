package score

import (
	"errors"

	scv "github.com/NJUPT-ISL/SCV/api/v1"
	"github.com/NJUPT-ISL/Yoda-Scheduler/pkg/yoda/collection"
	"github.com/NJUPT-ISL/Yoda-Scheduler/pkg/yoda/filter"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Sum is from collection/collection.go
// var Sum = []string{"Cores","FreeMemory","Bandwidth","MemoryClock","MemorySum","Number","Memory"}

type ResourceName string
type resourceToWeightMap map[v1.ResourceName]int64

const (
	BandwidthWeight   = 1
	ClockWeight       = 1
	CoreWeight        = 1
	PowerWeight       = 1
	FreeMemoryWeight  = 2
	TotalMemoryWeight = 1
	ActualWeight      = 2

	AllocateWeight = 3
)

func CalculateScore(s *scv.Scv, state *framework.CycleState, pod *v1.Pod, info *framework.NodeInfo) (uint64, error) {
	d, err := state.Read("Max")
	if err != nil {
		return 0, errors.New("Error Get CycleState Info Max Error: " + err.Error())
	}
	data, ok := d.(*collection.Data)
	if !ok {
		return 0, errors.New("The Type is not Data ")
	}
	return CalculateBasicScore(data.Value, s, pod) + CalculateAllocateScore(info, s) + CalculateActualScore(s), nil
}

func CalculateBasicScore(value collection.MaxValue, scv *scv.Scv, pod *v1.Pod) uint64 {
	var cardScore uint64
	if ok, number := filter.PodFitsNumber(pod, scv); ok {
		isFitsMemory, memory := filter.PodFitsMemory(number, pod, scv)
		isFitsClock, clock := filter.PodFitsClock(number, pod, scv)
		if isFitsClock && isFitsMemory {
			for _, card := range scv.Status.CardList {
				if card.FreeMemory >= memory && card.Clock >= clock {
					cardScore += CalculateCardScore(value, card)
				}
			}
		}
	}
	return cardScore
}

func CalculateCardScore(value collection.MaxValue, card scv.Card) uint64 {
	var (
		bandwidth   = card.Bandwidth * 100 / value.MaxBandwidth
		clock       = card.Clock * 100 / value.MaxBandwidth
		core        = card.Core * 100 / value.MaxCore
		power       = card.Power * 100 / value.MaxPower
		freeMemory  = card.FreeMemory * 100 / value.MaxFreeMemory
		totalMemory = card.TotalMemory * 100 / value.MaxTotalMemory
	)
	return uint64(bandwidth*BandwidthWeight+clock*ClockWeight+core*CoreWeight+power*PowerWeight) +
		freeMemory*FreeMemoryWeight + totalMemory*TotalMemoryWeight
}

func CalculateActualScore(scv *scv.Scv) uint64 {
	return (scv.Status.FreeMemorySum * 100 / scv.Status.TotalMemorySum) * ActualWeight
}

func CalculateAllocateScore(info *framework.NodeInfo, scv *scv.Scv) uint64 {
	allocateMemorySum := uint64(0)
	for _, pod := range info.Pods {
		if mem, ok := pod.Pod.GetLabels()["scv/memory"]; ok {
			allocateMemorySum += filter.StrToUint64(mem)
		}
	}

	if scv.Status.TotalMemorySum < allocateMemorySum {
		return 0
	}

	return (scv.Status.TotalMemorySum - allocateMemorySum) * 100 / scv.Status.TotalMemorySum * AllocateWeight
}

const (
	defaultWeight = int64(1)
)

func mostAllocatedScoreStrategy(requested, allocatable v1.ResourceList, resourceToWeightMap resourceToWeightMap) int64 {
	var numaNodeScore int64 = 0
	var weightSum int64 = 0

	for resourceName := range requested {
		// We don't care what kind of resources are being requested, we just iterate all of them.
		// If NUMA zone doesn't have the requested resource, the score for that resource will be 0.
		resourceScore := mostAllocatedScore(requested[resourceName], allocatable[resourceName])
		weight := resourceToWeightMap.weight(resourceName)
		numaNodeScore += resourceScore * weight
		weightSum += weight
	}

	return numaNodeScore / weightSum
}

// The used capacity is calculated on a scale of 0-MaxNodeScore (MaxNodeScore is
// constant with value set to 100).
// 0 being the lowest priority and 100 being the highest.
// The more allocated resources the node has, the higher the score is.
func mostAllocatedScore(requested, numaCapacity resource.Quantity) int64 {
	if numaCapacity.CmpInt64(0) == 0 {
		return 0
	}
	if requested.Cmp(numaCapacity) > 0 {
		return 0
	}

	return requested.Value() * framework.MaxNodeScore / numaCapacity.Value()
}

func (rw resourceToWeightMap) weight(r v1.ResourceName) int64 {
	w, ok := (rw)[r]
	if !ok {
		return defaultWeight
	}

	if w < 1 {
		return defaultWeight
	}

	return w
}
