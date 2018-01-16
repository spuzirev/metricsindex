package types

import (
	"fmt"

	"github.com/OneOfOne/xxhash"
)

type TagNameValue struct {
	TagName  TagName
	TagValue TagValue
}

func (tnv TagNameValue) ID() TagNameValueID {
	return TagNameValueID(xxhash.ChecksumString64(fmt.Sprintf("%s:%s", string(tnv.TagName), string(tnv.TagValue))))
}
