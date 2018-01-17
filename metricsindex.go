package metricsindex

import (
	"errors"

	"github.com/spuzirev/metricsindex/trees/metric_ids"

	"github.com/spuzirev/metricsindex/trees/tag_values"

	"github.com/spuzirev/metricsindex/trees/metric_id_to_metric"
	"github.com/spuzirev/metricsindex/trees/tag_name_id_to_metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_name_id_to_tag_values"
	"github.com/spuzirev/metricsindex/trees/tag_name_value_id_to_metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_names"
	"github.com/spuzirev/metricsindex/types"
)

var (
	ErrCannotParse         error = errors.New("Cannot parse metric")
	ErrNoSuchMetric        error = errors.New("No such metric")
	ErrSomeMetricsNotFound error = errors.New("Some metrics not found")
)

type MetricsIndex struct {
	MetricIDToMetric          *metric_id_to_metric.Tree
	TagNameIDToTagValues      *tag_name_id_to_tag_values.Tree
	TagNameIDToMetricIDs      *tag_name_id_to_metric_ids.Tree
	TagNameValueIDToMetricIDs *tag_name_value_id_to_metric_ids.Tree
	TagNames                  *tag_names.Tree
	MetricIDToBool            map[types.MetricID]bool
}

func NewMetricsIndex() *MetricsIndex {
	return &MetricsIndex{
		MetricIDToBool: make(map[types.MetricID]bool),
		MetricIDToMetric: metric_id_to_metric.TreeNew(func(a, b types.MetricID) int {
			return types.CmpMetricIDs(a, b)
		}),
		TagNameIDToTagValues: tag_name_id_to_tag_values.TreeNew(func(a, b types.TagNameID) int {
			return types.CmpTagNameIDs(a, b)
		}),
		TagNameIDToMetricIDs: tag_name_id_to_metric_ids.TreeNew(func(a, b types.TagNameID) int {
			return types.CmpTagNameIDs(a, b)
		}),
		TagNameValueIDToMetricIDs: tag_name_value_id_to_metric_ids.TreeNew(func(a, b types.TagNameValueID) int {
			return types.CmpTagNameValueID(a, b)
		}),
		TagNames: tag_names.TreeNew(func(a, b types.TagName) int {
			return types.CmpTagNames(a, b)
		}),
	}
}

type MetricIDIterator struct{}

func (midi *MetricIDIterator) Next() (types.MetricID, error) {
	return types.MetricID(0), nil
}

type TagNameIterator struct{}

func (tni *TagNameIterator) Next() (string, error) {
	return "", nil
}

type TagValueIterator struct{}

func (tvi *TagValueIterator) Next() (string, error) {
	return "", nil
}

func (mi *MetricsIndex) MetricExistsByMetricID(metricID types.MetricID) bool {
	_, ok := mi.MetricIDToBool[metricID]
	return ok
}

func (mi *MetricsIndex) MetricExistsByMetricStr(metricStr string) bool {
	metric, err := types.ParseMetric(metricStr)
	// if we unable to parse, that means we don't have that
	// "metric" in index, so we suppress the error from Parser
	if err != nil {
		return false
	}
	return mi.MetricExistsByMetricID(metric.ID())
}

func (mi *MetricsIndex) insertMetric(metric *types.Metric) error {
	metricID := metric.ID()
	if mi.MetricExistsByMetricID(metricID) {
		// this metric is already in index, return
		return nil
	}

	// MetricIDToBool
	mi.MetricIDToBool[metricID] = true

	// MetricIDToMetric
	mi.MetricIDToMetric.Set(metricID, *metric)

	// Tag* indexes
	for tn, tv := range metric.Tags {
		tagName := types.TagName(tn)
		tagValue := types.TagValue(tv)
		tnid := tagName.ID()

		var values *tag_values.Tree
		var metricIDs *metric_ids.Tree
		var ok bool

		// TagNameIDToTagValues
		if values, ok = mi.TagNameIDToTagValues.Get(tnid); !ok {
			values = tag_values.TreeNew(func(a, b types.TagValue) int {
				return types.CmpTagValues(a, b)
			})
			mi.TagNameIDToTagValues.Set(tnid, values)
		}
		values.Set(types.TagValue(tv), true)

		// TagNameIDToMetricIDs
		if metricIDs, ok = mi.TagNameIDToMetricIDs.Get(tnid); !ok {
			metricIDs = metric_ids.TreeNew(func(a, b types.MetricID) int {
				return types.CmpMetricIDs(a, b)
			})
			mi.TagNameIDToMetricIDs.Set(tnid, metricIDs)
		}
		metricIDs.Set(metricID, true)

		// TagNameValueIDToMetricIDs
		tnvid := types.TagNameValue{
			TagName:  tagName,
			TagValue: tagValue,
		}.ID()
		if metricIDs, ok = mi.TagNameValueIDToMetricIDs.Get(tnvid); !ok {
			metricIDs = metric_ids.TreeNew(func(a, b types.MetricID) int {
				return types.CmpMetricIDs(a, b)
			})
			mi.TagNameValueIDToMetricIDs.Set(tnvid, metricIDs)
		}
		metricIDs.Set(metricID, true)

		// TagNames
		mi.TagNames.Set(tagName, true)
	}
	return nil
}

func (mi *MetricsIndex) InsertMetric(metricStr string) error {
	metric, err := types.ParseMetric(metricStr)
	if err != nil {
		return err
	}
	return mi.insertMetric(metric)
}

func (mi *MetricsIndex) InsertMetricsBatch(metricsStr []string) error {
	for _, metricStr := range metricsStr {
		err := mi.InsertMetric(metricStr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mi *MetricsIndex) GetMetricIDsIteratorByTag(tagNameStr, tagValueStr string) (*MetricIDIterator, error) {
	return nil, nil
}

// GetCardinalityByTag returns total number of metrics which matches
// given condition.
// It returns 0 if there is no such tagNameStr:tagValueStr combination
func (mi *MetricsIndex) GetCardinalityByTag(tagNameStr, tagValueStr string) int {
	tnvid := types.TagNameValue{
		TagName:  types.TagName(tagNameStr),
		TagValue: types.TagValue(tagValueStr),
	}.ID()
	if v, ok := mi.TagNameValueIDToMetricIDs.Get(tnvid); ok {
		return v.Len()
	}
	return 0
}

// GetCardinalityByTagName returns total number of metric which has
// given tag.
// It returns 0 if there is no such tagNameStr in the index
func (mi *MetricsIndex) GetCardinalityByTagName(tagNameStr string) int {
	tnid := types.TagName(tagNameStr).ID()
	if v, ok := mi.TagNameIDToMetricIDs.Get(tnid); ok {
		return v.Len()
	}
	return 0
}

func (mi *MetricsIndex) GetTagNames(prefix string) []string {
	return nil
}

func (mi *MetricsIndex) GetTagNamesIterator(prefix string) (*TagNameIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetAllTagNames() []string {
	tagNameStrs := make([]string, mi.TagNames.Len())
	if len(tagNameStrs) == 0 {
		return tagNameStrs
	}
	var err error
	var tagName types.TagName
	i := 0
	for e, _ := mi.TagNames.SeekFirst(); err == nil; tagName, _, err = e.Next() {
		tagNameStrs[i] = string(tagName)
		i++
	}
	return tagNameStrs
}

func (mi *MetricsIndex) GetAllTagNamesIterator() (*TagNameIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetTagValues(tagNameStr, prefix string) []string {
	return nil
}

func (mi *MetricsIndex) GetTagValuesIterator(tagNameStr, prefix string) (*TagValueIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetAllTagValues(tagNameStr string) []string {
	return nil
}

func (mi *MetricsIndex) GetAllTagValuesIterator(tagNameStr string) (*TagValueIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetMetricNameByID(metricID types.MetricID) (string, error) {
	metric, ok := mi.MetricIDToMetric.Get(metricID)
	if !ok {
		return "", ErrNoSuchMetric
	}
	return metric.Serialize(), nil
}

func (mi *MetricsIndex) GetMetricsNamesByIDs(metricIDs []types.MetricID) ([]string, error) {
	metricStrs := make([]string, len(metricIDs))
	var errRes error
	for i, metricID := range metricIDs {
		metricStr, err := mi.GetMetricNameByID(metricID)
		if err != nil {
			errRes = ErrSomeMetricsNotFound
		}
		metricStrs[i] = metricStr
	}
	return metricStrs, errRes
}
