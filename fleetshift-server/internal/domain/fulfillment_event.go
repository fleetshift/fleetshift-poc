package domain

// DeliveryCompletionEvent records the terminal outcome of a single delivery.
type DeliveryCompletionEvent struct {
	DeliveryID DeliveryID
	Generation Generation
	Result     DeliveryResult
}

// DeliveryAckedEvent records that a delivery has been acknowledged by
// the addon -- i.e. it has transitioned out of [DeliveryStatePending].
// This is signaled to the workflow so it knows the addon received the
// work and can stop the ack retry loop for this delivery.
type DeliveryAckedEvent struct {
	DeliveryID DeliveryID
	Generation Generation
}

// FulfillmentEvent is the signal delivered to a running reconciliation
// workflow. Exactly one field is non-nil per event instance.
type FulfillmentEvent struct {
	DeliveryAcked     *DeliveryAckedEvent
	DeliveryCompleted *DeliveryCompletionEvent
}
