package sls

import "strconv"

func BoolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func BoolPtrToStringNum(b *bool) string {
	if b == nil {
		return ""
	}
	if *b {
		return "1"
	}
	return "0"
}

func Int64PtrToString(i *int64) string {
	if i == nil {
		return ""
	}
	return strconv.FormatInt(*i, 10)
}
