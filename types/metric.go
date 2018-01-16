package types

import (
	"sort"
	"strings"

	"github.com/OneOfOne/xxhash"
)

type Metric struct {
	Name string
	Tags map[string]string
}

func (m *Metric) Serialize() string {
	return string(m.SerializeToByteSlice())
}

func (m *Metric) String() string {
	return m.Serialize()
}

func (m *Metric) SerializeToByteSlice() []byte {
	b := make([]byte, 0)
	b = append(b, []byte(m.Name)...)

	tagNames := make([]string, 0)
	for k := range m.Tags {
		tagNames = append(tagNames, k)
	}
	sort.Slice(tagNames, func(i, j int) bool {
		return strings.Compare(tagNames[i], tagNames[j]) == -1
	})
	for _, tagName := range tagNames {
		b = append(b, []byte(";"+tagName+"="+m.Tags[tagName])...)
	}
	return b
}

func (m *Metric) Hash() uint64 {
	return xxhash.Checksum64(m.SerializeToByteSlice())
}

func (m *Metric) CalcMetricID() MetricID {
	return MetricID(m.Hash())
}
