package types

type TagNameID uint64

func CmpTagNameIDs(a, b TagNameID) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	} else {
		return 0
	}
}
