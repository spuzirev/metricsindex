package types

type TagNameValueID uint64

func CmpTagNameValueID(a, b TagNameValueID) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	} else {
		return 0
	}
}
