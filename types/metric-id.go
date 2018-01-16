package types

type MetricID uint64

func CmpMetricIDs(a, b MetricID) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	} else {
		return 0
	}
}
