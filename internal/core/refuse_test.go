package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestRefuseClosesWithConnectionRefused(t *testing.T) {
	s := capacitySession()
	s.Refuse("memory budget")
	if e := s.Err(); e == nil || e.Kind != CloseTransport || e.Code != wire.ConnectionRefused {
		t.Fatalf("err %+v", e)
	}
}
