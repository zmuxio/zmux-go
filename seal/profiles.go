package seal

import "github.com/zmuxio/zmux-go/seal/proto"

// The choices of profile of both ends, in one place.

var (
	// clientProfiles is a client's default list, without and with AES hardware.
	clientProfiles    = []proto.Profile{proto.ProfileEstablishPSK, proto.ProfileEstablish, proto.ProfileEstablishPSKAES, proto.ProfileEstablishAES}
	clientProfilesAES = []proto.Profile{proto.ProfileEstablishPSKAES, proto.ProfileEstablishAES, proto.ProfileEstablishPSK, proto.ProfileEstablish}

	// serverProfiles is what a server accepts by default: the Profiles with a
	// pre-shared key.
	serverProfiles = []proto.Profile{proto.ProfileEstablishPSK, proto.ProfileEstablishPSKAES}
)

func defaultClientProfiles() []proto.Profile {
	if hasAESHardware() {
		return clientProfilesAES
	}
	return clientProfiles
}
