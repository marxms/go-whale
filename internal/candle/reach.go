package candle

import "time"

// ResidentReach is HOW FAR BACK a timeframe's hot ZSET reaches in time:
// members x timeframe duration.
//
// The member count is the MAX between the inherited depth and the declared cap. The
// inherited depth is the floor a cell retains when nobody declares an entry for it,
// and using the max is what makes the answer immune to the cap key: an entry can only
// ADD reach, never remove the question.
//
// false means the timeframe cannot be measured. The caller decides, and neither
// branch decides "so it is fine": the guard includes the cell, while the deliberate
// horizon ignores it rather than inventing a reach.
func ResidentReach(timeframe string, inheritedMembers, declaredMembers int) (time.Duration, bool) {
	bucketMs, measurable := ParseTimeframeMs(timeframe)
	if !measurable || bucketMs <= 0 {
		return 0, false
	}
	members := inheritedMembers
	if declaredMembers > members {
		members = declaredMembers
	}
	return time.Duration(int64(members)*bucketMs) * time.Millisecond, true
}
