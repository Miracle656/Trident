package main

import "sync"

// Issue #454: deliveries for different subscriptions matching the same
// event used to run one at a time in a single loop, so a single hanging
// endpoint delayed every other subscriber behind it. globalDeliverySem
// bounds total concurrent deliveries across the process; inFlightSubs
// caps concurrency per subscription at 1, so one flaky endpoint can't pile
// up multiple in-flight deliveries against itself and monopolize the
// global pool while other subscriptions wait.
var globalDeliverySem = make(chan struct{}, 20)

var (
	inFlightMu   sync.Mutex
	inFlightSubs = make(map[string]bool)
)

// tryAcquireSubscriptionSlot reports whether a delivery for subscriptionID
// may proceed now (no delivery already in flight for it), reserving the
// slot if so. Pairs with releaseSubscriptionSlot.
func tryAcquireSubscriptionSlot(subscriptionID string) bool {
	inFlightMu.Lock()
	defer inFlightMu.Unlock()
	if inFlightSubs[subscriptionID] {
		return false
	}
	inFlightSubs[subscriptionID] = true
	return true
}

func releaseSubscriptionSlot(subscriptionID string) {
	inFlightMu.Lock()
	defer inFlightMu.Unlock()
	delete(inFlightSubs, subscriptionID)
}
