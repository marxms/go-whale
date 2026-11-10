package market

import "testing"

func TestConnPolicyCoversFourScopes(t *testing.T) {
	scopes := []struct {
		exchange string
		mt       MarketType
	}{
		{"binance", Spot},
		{"binance", Future},
		{"bingx", Spot},
		{"bingx", Future},
	}
	for _, scope := range scopes {
		policy, ok := ConnPolicyFor(scope.exchange, scope.mt)
		if !ok {
			t.Fatalf("ConnPolicyFor(%s, %s) = not found", scope.exchange, scope.mt)
		}
		if policy.DocsURL == "" {
			t.Errorf("ConnPolicyFor(%s, %s) has no DocsURL", scope.exchange, scope.mt)
		}
		if policy.ServerPing <= 0 {
			t.Errorf("ConnPolicyFor(%s, %s) has no documented server ping", scope.exchange, scope.mt)
		}
		if policy.MaxStreamsPerConn <= 0 {
			t.Errorf("ConnPolicyFor(%s, %s) has no documented stream limit", scope.exchange, scope.mt)
		}
	}
}

func TestConnPolicyFieldsRequireDocsURL(t *testing.T) {
	scopes := []struct {
		exchange string
		mt       MarketType
	}{
		{"binance", Spot},
		{"binance", Future},
		{"bingx", Spot},
		{"bingx", Future},
	}
	for _, scope := range scopes {
		policy, _ := ConnPolicyFor(scope.exchange, scope.mt)
		if policy.DocsURL != "" {
			continue
		}
		if hasDocumentedField(policy) {
			t.Errorf("ConnPolicyFor(%s, %s) declares fields without a DocsURL", scope.exchange, scope.mt)
		}
	}
}

func TestConnPolicyRoutesMatchStreamClass(t *testing.T) {
	futures, _ := ConnPolicyFor("binance", Future)
	want := map[string]string{
		"trade":    "/public",
		"kline":    "/market",
		"aggTrade": "/market",
	}
	for class, route := range want {
		if got := futures.Routes[class]; got != route {
			t.Errorf("futures route for %s = %q, want %q", class, got, route)
		}
	}
	spot, _ := ConnPolicyFor("binance", Spot)
	if len(spot.Routes) != 0 {
		t.Errorf("spot routes = %v, want none", spot.Routes)
	}
}

func TestBingXKeepaliveEncoding(t *testing.T) {
	spot, _ := ConnPolicyFor("bingx", Spot)
	if spot.PingEncoding != PingEncodingJSON {
		t.Errorf("bingx spot ping encoding = %q, want %q", spot.PingEncoding, PingEncodingJSON)
	}
	futures, _ := ConnPolicyFor("bingx", Future)
	if futures.PingEncoding != PingEncodingText {
		t.Errorf("bingx futures ping encoding = %q, want %q", futures.PingEncoding, PingEncodingText)
	}
	binance, _ := ConnPolicyFor("binance", Future)
	if binance.PingEncoding != PingEncodingProtocol {
		t.Errorf("binance ping encoding = %q, want %q", binance.PingEncoding, PingEncodingProtocol)
	}
}

func hasDocumentedField(policy ConnPolicy) bool {
	return policy.ServerPing != 0 ||
		policy.PongTimeout != 0 ||
		policy.ClientPing != 0 ||
		policy.ReadIdle != 0 ||
		policy.MaxLifetime != 0 ||
		policy.MaxStreamsPerConn != 0 ||
		policy.MaxConnsPerIP != 0 ||
		policy.ConnWindow != 0 ||
		policy.PingEncoding != "" ||
		len(policy.Routes) != 0
}
