package types

import (
	"strings"

	"github.com/OneOfOne/xxhash"
)

type TagName string

func CmpTagNames(a, b TagName) int {
	return strings.Compare(string(a), string(b))
}

func (tn TagName) ID() TagNameID {
	return TagNameID(xxhash.ChecksumString64(string(tn)))
}
