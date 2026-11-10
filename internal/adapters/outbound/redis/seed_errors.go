package redisstore

import "errors"

// ErrSeedHistoryUnavailable marks a failed protective history read. The seeder must
// retry storage availability without declaring the symbol invalid or throttling REST.
var ErrSeedHistoryUnavailable = errors.New("seed history unavailable")

// ErrSeedColdAdmissionUnavailable identifies an unadmitted cold write, not an
// invalid symbol. It does not promise that an automatic retry will occur.
var ErrSeedColdAdmissionUnavailable = errors.New("seed cold admission unavailable")
