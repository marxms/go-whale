package app

// appOption injects a seam BEFORE construction, for dependencies that `NewApp` builds
// itself and therefore cannot be swapped afterwards — `serverProbe` and `durableOrder`
// are consumed in `Run` and admit field assignment; the durable-tier constructor is
// consumed INSIDE `NewApp` and does not.
//
// The type is unexported on purpose: no caller outside this package can pass an option,
// which is the right scope for a test seam.
type appOption func(*App)

// withColdTierConstructor swaps the durable-tier constructor. See App.newColdTier.
func withColdTierConstructor(construct questDBConstructor) appOption {
	return func(a *App) { a.newColdTier = construct }
}
