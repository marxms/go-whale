package storetest

import "testing"

// The fake is the extensibility proof: it implements only the ports and still
// satisfies every case in the suite. An adapter that needs the suite to know
// about its driver has not been narrowed enough.
func TestFakeHot_SatisfiesTheContractSuite(t *testing.T) {
	RunHot(t, func(_ *testing.T) HotStore { return NewFakeHot(5) }, HotOptions{Cap: 5})
}

func TestFakeCold_SatisfiesTheContractSuite(t *testing.T) {
	RunCold(t, func(_ *testing.T) ColdStore { return NewFakeCold() })
}
