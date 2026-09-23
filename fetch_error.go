package spark

import "fmt"

// FetchError identifies an accepted map attempt whose output could not be read.
// It is preserved across worker RPCs so recovery can target the lost lineage.
type FetchError struct {
	JobID     string `json:"job_id"`
	ShuffleID int    `json:"shuffle_id"`
	MapID     int    `json:"map_id"`
	Attempt   int    `json:"attempt"`
	Reason    string `json:"reason"`
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("shuffle %d map %d attempt %d unavailable: %s", e.ShuffleID, e.MapID, e.Attempt, e.Reason)
}
