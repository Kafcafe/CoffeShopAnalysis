package join

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE = 20
	SINGLE_ITEM_BUFFER_LEN    = 1

	ACK          = 0
	NACK_REQUEUE = 1
	NACK_DISCARD = 2

	ACTIVITY = 0
)

func answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
		message.Ack(false)
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}
