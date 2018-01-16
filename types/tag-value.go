package types

import (
	"strings"
)

type TagValue string

func CmpTagValues(a, b TagValue) int {
	return strings.Compare(string(a), string(b))
}
