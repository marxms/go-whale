package storetest

import "testing"

// The fakes are the extensibility proof: the suite must pass with nothing but
// the ports, then an adapter runs the same cases against its real driver.
func TestFakeHot_SatisfiesTheContractSuite(t *testing.T) {
	RunHot(t, func(_ *testing.T) HotStore { return NewFakeHot() })
}

func TestFakeBaseline_SatisfiesTheContractSuite(t *testing.T) {
	RunBaseline(t, func(_ *testing.T) BaselineStore { return NewFakeHot() })
}

func TestFakeCold_SatisfiesTheContractSuite(t *testing.T) {
	RunCold(t, func(_ *testing.T) ColdStore { return NewFakeCold() })
}
