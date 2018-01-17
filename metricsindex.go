package metricsindex

import (
	"github.com/spuzirev/metricsindex/trees/metric_id_to_metric"
	"github.com/spuzirev/metricsindex/trees/tag_name_id_to_metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_name_id_to_tag_values"
	"github.com/spuzirev/metricsindex/trees/tag_name_value_id_to_metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_names"
	"github.com/spuzirev/metricsindex/types"
)

type MetricsIndex struct {
	MetricIDToMetric          *metric_id_to_metric.Tree
	TagNameIDToTagValues      *tag_name_id_to_tag_values.Tree
	TagNameIDToMetricIDs      *tag_name_id_to_metric_ids.Tree
	TagNameValueIDToMetricIDs *tag_name_value_id_to_metric_ids.Tree
	TagNames                  *tag_names.Tree
}

func NewMetricsIndex() *MetricsIndex {
	return &MetricsIndex{
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

func (mi *MetricsIndex) insertMetric(metric types.Metric) error {
	return nil
}

func (mi *MetricsIndex) InsertMetric(metricStr string) error {
	return nil
}

func (mi *MetricsIndex) InsertMetricsBatch(metricStr []string) error {
	return nil
}

func (mi *MetricsIndex) GetMetricIDsIteratorByTag(tagNameStr, tagValueStr string) (*MetricIDIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetCardinalityByTag(tagNameStr, tagValueStr string) int {
	return 0
}

func (mi *MetricsIndex) GetCardinalityByTagName(tagNameStr string) int {
	return 0
}

func (mi *MetricsIndex) GetTagNames(prefix string) []string {
	return nil
}

func (mi *MetricsIndex) GetTagNamesIterator(prefix string) (*TagNameIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetAllTagNames() []string {
	return nil
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

func (mi *MetricsIndex) GetMetricNameByID(id types.MetricID) (string, error) {
	return "", nil
}

func (mi *MetricsIndex) GetMetricsNamesByIDs(ids []types.MetricID) ([]string, error) {
	return nil, nil
}
