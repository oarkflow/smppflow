package lifecycle

type ConnectionState string

const (
	ConnNew          ConnectionState = "new"
	ConnConnecting                   = "connecting"
	ConnConnected                    = "connected"
	ConnBinding                      = "binding"
	ConnBound                        = "bound"
	ConnActive                       = "active"
	ConnDraining                     = "draining"
	ConnUnbinding                    = "unbinding"
	ConnClosed                       = "closed"
	ConnReconnecting                 = "reconnecting"
	ConnFailed                       = "failed"
)

type MessageState string

const (
	MsgCreated            MessageState = "created"
	MsgValidated                       = "validated"
	MsgEncoded                         = "encoded"
	MsgSegmented                       = "segmented"
	MsgQueued                          = "queued"
	MsgSending                         = "sending"
	MsgSubmitted                       = "submitted"
	MsgAccepted                        = "accepted"
	MsgRejected                        = "rejected"
	MsgThrottled                       = "throttled"
	MsgDelivered                       = "delivered"
	MsgUndeliverable                   = "undeliverable"
	MsgFailed                          = "failed"
	MsgExpired                         = "expired"
	MsgPartiallyDelivered              = "partially_delivered"
	MsgUnknown                         = "unknown"
	MsgDeadLettered                    = "dead_lettered"
	MsgArchived                        = "archived"
)
