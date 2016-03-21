package redisc

import "strings"

// HashSlotForKey returns the hash slot for key.
func HashSlotForKey(key string) int {
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+1:], "}"); end > 0 { // if end == 0, then it's {}, so we ignore it
			end += start + 1
			key = key[start+1 : end]
		}
	}
	return int(crc16(key) % hashSlots)
}
