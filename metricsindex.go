package metricsindex

import (
	"errors"
	"io"
	"strings"

	"github.com/spuzirev/metricsindex/trees/metric_id_to_metric"
	"github.com/spuzirev/metricsindex/trees/metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_name_id_to_metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_name_id_to_tag_values"
	"github.com/spuzirev/metricsindex/trees/tag_name_value_id_to_metric_ids"
	"github.com/spuzirev/metricsindex/trees/tag_names"
	"github.com/spuzirev/metricsindex/trees/tag_values"
	"github.com/spuzirev/metricsindex/types"
)

var (
	// ErrNoSuchMetric represents situation when metric not found
	ErrNoSuchMetric = errors.New("No such metric")

	// ErrNoSuchTag represents situation when tag not found
	ErrNoSuchTag = errors.New("No such tag")

	// ErrNoSuchTagNameValue represents situation when tagName:tagValue not found
	ErrNoSuchTagNameValue = errors.New("No such tagName:tagValue")

	// ErrSomeMetricsNotFound represents situation when some of requested
	// metrics not found
	ErrSomeMetricsNotFound = errors.New("Some metrics not found")
)

// MetricsIndex is the main Index object
type MetricsIndex struct {
	MetricIDToMetric          *metric_id_to_metric.Tree
	TagNameIDToTagValues      *tag_name_id_to_tag_values.Tree
	TagNameIDToMetricIDs      *tag_name_id_to_metric_ids.Tree
	TagNameValueIDToMetricIDs *tag_name_value_id_to_metric_ids.Tree
	TagNames                  *tag_names.Tree
	MetricIDToBool            map[types.MetricID]bool
}

// NewMetricsIndex is *MetricsIndex builder and initializer
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

// MetricIDIterator is iterator over type.MetricID
type MetricIDIterator struct {
	e *metric_ids.Enumerator
}

// Next returns item if it exists and moves to next position
// If there is no item to return err == io.EOF is returned
func (midi *MetricIDIterator) Next() (types.MetricID, error) {
	k, _, err := midi.e.Next()
	return k, err
}

// Close closes the MetricIDIterator
func (midi *MetricIDIterator) Close() {
	midi.e.Close()
}

// TagNameIterator is iterator over type.TagName
type TagNameIterator struct {
	e       *tag_names.Enumerator
	filter  func(k types.TagName) bool
	eofSent bool
}

// Next returns item if it exists and moves to next position
// If there is no item to return err == io.EOF is returned
func (tni *TagNameIterator) Next() (string, error) {
	if tni.eofSent {
		return "", io.EOF
	}
	k, _, err := tni.e.Next()
	if !tni.filter(k) {
		tni.eofSent = true
		return "", io.EOF
	}
	return string(k), err
}

// Close closes TagNameIterator
func (tni *TagNameIterator) Close() {
	tni.e.Close()
	tni.eofSent = true
}

// TagValueIterator is iterator over type.TagValue
type TagValueIterator struct {
	e       *tag_values.Enumerator
	filter  func(k types.TagValue) bool
	eofSent bool
}

// Next returns item if it exists and moves to next position
// If there is no item to return err == io.EOF is returned
func (tvi *TagValueIterator) Next() (string, error) {
	if tvi.eofSent {
		return "", io.EOF
	}
	k, _, err := tvi.e.Next()
	if !tvi.filter(k) {
		tvi.eofSent = true
		return "", io.EOF
	}
	return string(k), err
}

// Close closes TagValueIterator
func (tvi *TagValueIterator) Close() {
	tvi.e.Close()
	tvi.eofSent = true
}

// MetricExistsByMetricID returns true if metric with given metricID
// exists in the index, otherwise it returns false
func (mi *MetricsIndex) MetricExistsByMetricID(metricID types.MetricID) bool {
	_, ok := mi.MetricIDToBool[metricID]
	return ok
}

//MetricExistsByMetricStr returns true if metric with given full name (with tags)
// exists in the index, otherwise it returns false
func (mi *MetricsIndex) MetricExistsByMetricStr(metricStr string) bool {
	metric, err := types.ParseMetric(metricStr)
	// if we unable to parse, that means we don't have that
	// "metric" in index, so we suppress the error from Parser
	if err != nil {
		return false
	}
	return mi.MetricExistsByMetricID(metric.ID())
}

// insertMetric is internal method which inserts new types.Metric to index
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

// InsertMetric inserts new metric to index by metric string representation
// it may return error if fails
func (mi *MetricsIndex) InsertMetric(metricStr string) error {
	metric, err := types.ParseMetric(metricStr)
	if err != nil {
		return err
	}
	return mi.insertMetric(metric)
}

// InsertMetricsBatch takes slice of metric strings representations
// and inserts them to index
func (mi *MetricsIndex) InsertMetricsBatch(metricsStr []string) error {
	for _, metricStr := range metricsStr {
		err := mi.InsertMetric(metricStr)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetMetricIDsIteratorByTag returns MetricIDIterator for given
// tagNameStr:tagValueStr pair
func (mi *MetricsIndex) GetMetricIDsIteratorByTag(tagNameStr, tagValueStr string) (*MetricIDIterator, error) {
	tvid := types.TagNameValue{
		TagName:  types.TagName(tagNameStr),
		TagValue: types.TagValue(tagValueStr),
	}.ID()
	metricIDs, ok := mi.TagNameValueIDToMetricIDs.Get(tvid)
	if !ok {
		return nil, ErrNoSuchTagNameValue
	}
	e, _ := metricIDs.SeekFirst()
	iterator := &MetricIDIterator{
		e: e,
	}
	return iterator, nil
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

// GetTagNames returns slice of strings representing all possible
// names of tags in the index with prefix
// If there is no metric with given prefix empty slice is returned
func (mi *MetricsIndex) GetTagNames(prefix string) []string {
	res := make([]string, 0)
	var err error
	var e *tag_names.Enumerator
	var tagName types.TagName

	e, _ = mi.TagNames.Seek(types.TagName(prefix))
	defer e.Close()
	for {
		tagName, _, err = e.Next()
		if err == io.EOF {
			break
		}
		tagNameStr := string(tagName)
		if strings.HasPrefix(tagNameStr, prefix) {
			res = append(res, tagNameStr)
		} else {
			break
		}

	}
	return res
}

// GetTagNamesIterator returns a *TagNameIterator which will return
// all tag names with a given prefix
func (mi *MetricsIndex) GetTagNamesIterator(prefix string) (*TagNameIterator, error) {
	e, _ := mi.TagNames.Seek(types.TagName(prefix))
	iterator := &TagNameIterator{
		e:       e,
		eofSent: false,

		filter: func(k types.TagName) bool {
			return strings.HasPrefix(string(k), prefix)
		},
	}
	return iterator, nil
}

// GetAllTagNames is shortcut for GetTagNames("")
func (mi *MetricsIndex) GetAllTagNames() []string {
	return mi.GetTagNames("")
}

// GetAllTagNamesIterator returns a *TagNameIterator over all tags in index
func (mi *MetricsIndex) GetAllTagNamesIterator() (*TagNameIterator, error) {
	e, err := mi.TagNames.SeekFirst()
	if err != nil {
		return nil, err
	}
	iterator := &TagNameIterator{
		e:       e,
		eofSent: false,

		filter: func(k types.TagName) bool {
			return true
		},
	}
	return iterator, nil
}

// GetTagValues return slice of strings representing all possible
// values for given tagNameStr in the index
func (mi *MetricsIndex) GetTagValues(tagNameStr, prefix string) []string {
	tnid := types.TagName(tagNameStr).ID()

	res := make([]string, 0)
	var err error
	var e *tag_values.Enumerator
	var tagValues *tag_values.Tree
	var tagValue types.TagValue
	var ok bool

	// if no such tag return empty slice
	if tagValues, ok = mi.TagNameIDToTagValues.Get(tnid); !ok {
		return res
	}

	e, _ = tagValues.Seek(types.TagValue(prefix))
	defer e.Close()
	for {
		tagValue, _, err = e.Next()
		if err == io.EOF {
			break
		}
		tagValueStr := string(tagValue)
		if strings.HasPrefix(tagValueStr, prefix) {
			res = append(res, tagValueStr)
		} else {
			break
		}

	}
	return res
}

// GetTagValuesIterator returns a *TagValueIterator which will return
// all tag values with a given prefix for given tag
func (mi *MetricsIndex) GetTagValuesIterator(tagNameStr, prefix string) (*TagValueIterator, error) {
	tagValues, ok := mi.TagNameIDToTagValues.Get(types.TagName(tagNameStr).ID())
	if !ok {
		return nil, ErrNoSuchTag
	}
	e, _ := tagValues.Seek(types.TagValue(prefix))
	iterator := &TagValueIterator{
		e:       e,
		eofSent: false,

		filter: func(k types.TagValue) bool {
			return strings.HasPrefix(string(k), prefix)
		},
	}
	return iterator, nil
}

// GetAllTagValues is a shortcut for GetTagValues(tagNameStr, "")
func (mi *MetricsIndex) GetAllTagValues(tagNameStr string) []string {
	return mi.GetTagValues(tagNameStr, "")
}

// GetAllTagValuesIterator returns a *TagNameIterator over all tag values for given tag
func (mi *MetricsIndex) GetAllTagValuesIterator(tagNameStr string) (*TagValueIterator, error) {
	tagValues, ok := mi.TagNameIDToTagValues.Get(types.TagName(tagNameStr).ID())
	if !ok {
		return nil, ErrNoSuchTag
	}
	e, _ := tagValues.SeekFirst()
	iterator := &TagValueIterator{
		e:       e,
		eofSent: false,

		filter: func(k types.TagValue) bool {
			return true
		},
	}
	return iterator, nil
}

// GetMetricNameByID suddenly returns metric name by metricID
func (mi *MetricsIndex) GetMetricNameByID(metricID types.MetricID) (string, error) {
	metric, ok := mi.MetricIDToMetric.Get(metricID)
	if !ok {
		return "", ErrNoSuchMetric
	}
	return metric.Serialize(), nil
}

// GetMetricsNamesByIDs is a batch version of GetMetricNameByID
func (mi *MetricsIndex) GetMetricsNamesByIDs(metricIDs []types.MetricID) ([]string, error) {
	res := make([]string, len(metricIDs))
	var errRes error
	for i, metricID := range metricIDs {
		metricStr, err := mi.GetMetricNameByID(metricID)
		if err != nil {
			errRes = ErrSomeMetricsNotFound
		}
		res[i] = metricStr
	}
	return res, errRes
}
