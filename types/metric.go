package types

import (
	"errors"
	"sort"
	"strings"

	"github.com/OneOfOne/xxhash"
)

var (
	ErrCannotParseMetricName error = errors.New("Cannot parse metric name")
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

func Parse(metricStr string) (*Metric, error) {
	tokens := strings.Split(metricStr, ";")
	name := tokens[0]
	tags := make(map[string]string)
	if len(tokens) > 1 {
		tokens = tokens[1:]
		for _, token := range tokens {
			tntv := strings.Split(token, "=")
			if len(tntv) != 2 {
				return nil, ErrCannotParseMetricName
			}
			tags[tntv[0]] = tntv[1]
		}
	}
	return &Metric{
		Name: name,
		Tags: tags,
	}, nil
}
