package uni_func

import "strings"

func TruncateText(s string, subStr string) string {
	return s[:strings.LastIndex(s, subStr)]
}

func InterfaceEqual(a []interface{}, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for index := range a {
		if a[index].(string) != b[index].(string) {
			return false
		}
	}
	return true
}
