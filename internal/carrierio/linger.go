package carrierio

import "time"

// MaxLinger bounds graceful carrier shutdown.
const MaxLinger = 2 * time.Second

// Linger gives a carrier up to three round trips to drain after closing.
func Linger(srtt time.Duration) time.Duration {
	if srtt > MaxLinger/3 {
		return MaxLinger
	}
	return max(250*time.Millisecond, 3*srtt)
}
