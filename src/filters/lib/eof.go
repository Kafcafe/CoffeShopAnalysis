package filters

import (
	"common/middleware"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (f *FilterGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed int, emitted int, timeout bool) {
	for retriesCount := 0; processed < expectedEmitted && retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := f.middlewareHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		f.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-f.resultsChans[clientId][dataType]:
				f.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
				processed += msg.Processed
				emitted += msg.Emitted
			case <-time.After(timeoutDuration):
				f.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC)
				timeout = true
			}
		}
	}
	return processed, emitted, timeout
}

func (f *FilterGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	f.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	mh, err := middleware.NewMiddlewareHandler(f.middlewareHandler.RabbitConn)
	if err != nil {
		f.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	queueName := fmt.Sprintf("filter.%s.results.request.gather.%s", f.conf.ofType, eofMsg.ClientId)
	queue, err := mh.CreateQueue(queueName)
	if err != nil {
		f.log.Errorf("Failed to create ephemeral queue %s: %v", queueName, err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	requestMsg := middleware.NewGatherResultsRequest(f.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	requestBytes, err := requestMsg.ToBytes()
	if err != nil {
		f.log.Errorf("Failed to serialize results request message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	f.ensureResultsChanExists(eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(f.processResultsResponse, f.errChan)

	f.watchMesh.TryCrash("eof - before requesting results")

	processed, emitted, timeout := f.broadcastAndWaitForResults(requestBytes, eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	if processed == 0 {
		f.log.Errorf("Unexpected error waiting results for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	if timeout {
		f.log.Warningf("Could not gather all results for client %s and dataType %s after %d retries. Proceeding with partial results: processed %d/%d, emitted %d", eofMsg.ClientId, eofMsg.DataType, middleware.MAX_EOF_RETRIES, processed, eofMsg.TotalEmitted, emitted)
	}
	if err := queue.StopConsuming(); err != middleware.MessageMiddlewareSuccess {
		f.log.Warningf("Failed to stop consuming ephemeral queue %s: %v", queueName, err)
	}
	if err := queue.Delete(); err != middleware.MessageMiddlewareSuccess {
		f.log.Warningf("Failed to delete ephemeral queue %s: %v", queueName, err)
	}

	f.watchMesh.TryCrash("eof - after requesting results - before sending next stage")

	expectedTotal := eofMsg.TotalEmitted
	eofMsg.TotalEmitted = emitted
	if err := f.sendNextStage(eofMsg); err != nil {
		f.log.Errorf("Failed to send EOF message to next stage: %v. Requeuing message...", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	f.watchMesh.TryCrash("eof - after sending next stage - before ack")

	f.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Processed count: %d/%d, Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, processed, expectedTotal, emitted)
	answerMessage(ACK, eofMessage)

	clearMsg := middleware.NewClearResultsRequest(f.conf.id, "", eofMsg.ClientId, eofMsg.DataType)
	clearMsgBytes, err := clearMsg.ToBytes()
	if err != nil {
		f.log.Warningf("Failed to serialize results request message: %v", err)
	}
	if sendErr := f.middlewareHandlers.broadcastResultsRequestPub.Send(clearMsgBytes); sendErr != middleware.MessageMiddlewareSuccess {
		f.log.Warningf("Failed to send results request message to broadcast exchange: %v", sendErr)
	}
}

func (f *FilterGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	f.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
	f.resultsChans[msg.ClientId][msg.DataType] <- *msg
	answerMessage(ACK, message)
	return nil
}

func (f *FilterGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		f.getClientStats(msg.ClientId).Clear(msg.DataType)
		if f.getClientStats(msg.ClientId).IsEmpty() {
			if _, err := f.atomicWritter.CleanClient(msg.ClientId); err != nil {
				f.log.Errorf("failed to clean atomic writter for client %s: %v", msg.ClientId, err)
			}
		}
		f.log.Infof("Cleared stats for client %s and datatype %s", msg.ClientId, msg.DataType)
		answerMessage(ACK, message)
		return nil
	}

	f.log.Infof("Received results request message from %s for client %s and datatype %s", msg.Origin, msg.ClientId, msg.DataType)
	processed, emitted := f.getClientStats(msg.ClientId).GetStats(msg.DataType)

	f.watchMesh.TryCrash("gather results - before sending response")

	responseMsg := middleware.MessageResultsResponse{
		Origin:    f.conf.id,
		ClientId:  msg.ClientId,
		DataType:  msg.DataType,
		Processed: processed,
		Emitted:   emitted,
	}

	responseBytes, err := responseMsg.ToBytes()
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if sendErr := f.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	f.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
	return nil
}
