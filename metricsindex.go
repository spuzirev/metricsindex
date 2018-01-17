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

func (mi *MetricsIndex) insertMetric(metric types.Metric) error {
	return nil
}

func (mi *MetricsIndex) InsertMetric(metricStr string) error {
	return nil
}

func (mi *MetricsIndex) InsertMetricsBatch(metricStr []string) error {
	return nil
}

type MetricIDIterator struct{}

func (midi *MetricIDIterator) Next() (types.MetricID, error) {
	return types.MetricID(0), nil
}

func (mi *MetricsIndex) GetMetricIDsIteratorByTag(tagNameStr, tagValueStr string) (*MetricIDIterator, error) {
	return nil, nil
}

func (mi *MetricsIndex) GetTagNames(prefix string) []string {
	return nil
}

func (mi *MetricsIndex) GetAllTagNames() []string {
	return nil
}

func (mi *MetricsIndex) GetTagValues(tagNameStr, prefix string) []string {
	return nil
}

func (mi *MetricsIndex) GetAllTagValues(tagNameStr string) []string {
	return nil
}

func (mi *MetricsIndex) GetMetricNameByID(id types.MetricID) (string, error) {
	return "", nil
}

func (mi *MetricsIndex) GetMetricsNamesByIDs(ids []types.MetricID) ([]string, error) {
	return nil, nil
}

/*
func (mi *MetricsIndex) AddMetric(metric types.Metric) {
	metricID := metric.CalcMetricID()

	// if it's new metric
	if _, ok := mi.MetricIDToMetric.Get(metricID); !ok {
		mi.MetricIDToMetric.Set(metricID, metric)
		for tagNameStr, tagValueStr := range metric.Tags {
			tagName := types.TagName(tagNameStr)
			tagNameID := tagName.ID()

			tagValue := types.TagValue(tagValueStr)

			tagNameValue := types.TagNameValue{
				TagName:  tagName,
				TagValue: tagValue,
			}
			tagNameValueID := tagNameValue.ID()

			// if it's new tag in TagNameIDToTagValues
			var ok bool
			var v *tag_values.Tree
			if v, ok = mi.TagNameIDToTagValues.Get(tagNameID); !ok {
				v = tag_values.TreeNew(func(a, b types.TagValue) int {
					return types.CmpTagValues(a, b)
				})
				mi.TagNameIDToTagValues.Set(tagNameID, v)
			}
			v.Set(tagValue, true)

			// if it's new tag in TagNameIDToMetricIDs
			var v1 *metric_ids.Tree
			if v1, ok = mi.TagNameIDToMetricIDs.Get(tagNameID); !ok {
				v1 = metric_ids.TreeNew(func(a, b types.MetricID) int {
					return types.CmpMetricIDs(a, b)
				})
				mi.TagNameIDToMetricIDs.Set(tagNameID, v1)
			}
			v1.Set(metricID, true)

			// if it's new tagvalueid in TagNameValueIDToMetricIDs
			var v2 *metric_ids.Tree
			if v2, ok = mi.TagNameValueIDToMetricIDs.Get(tagNameValueID); !ok {
				v2 = metric_ids.TreeNew(func(a, b types.MetricID) int {
					return types.CmpMetricIDs(a, b)
				})
				mi.TagNameValueIDToMetricIDs.Set(tagNameValueID, v2)
			}
			v2.Set(metricID, true)

			// if it's new TagName in TagNames
			if _, ok := mi.TagNames.Get(tagName); !ok {
				mi.TagNames.Set(tagName, true)
			}
		}
	}
}

func (mi *MetricsIndex) GetEnumeratorByCondition(tn types.TagName, tv types.TagValue) (*metric_ids.Enumerator, error) {
	tvid := types.TagNameValue{
		TagName:  tn,
		TagValue: tv,
	}.ID()
	metricIDs, ok := mi.TagNameValueIDToMetricIDs.Get(tvid)
	if !ok {
		return nil, errors.New("No such element")
	}
	enumerator, err := metricIDs.SeekFirst()
	if err != nil {
		return nil, err
	}
	return enumerator, nil
}

*/
