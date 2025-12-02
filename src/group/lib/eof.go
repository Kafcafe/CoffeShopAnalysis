package group

import (
	"common/middleware"
	"fmt"
	"group/structures"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (g *GroupByGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	g.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	mh, err := middleware.NewMiddlewareHandler(g.middlewareHandler.RabbitConn)
	if err != nil {
		g.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	queueName := fmt.Sprintf("group.%s.results.request.gather.%s", g.conf.ofType, eofMsg.ClientId)
	queue, err := mh.CreateQueue(queueName)
	if err != nil {
		g.log.Errorf("Failed to create ephemeral queue %s: %v", queueName, err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	g.ensureResultsChanExists(eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(g.processResultsResponse, g.errChan)

	// GATHER AND SEND
	gatherMsg := middleware.NewGatherResultsRequest(g.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	gatherBytes, err := gatherMsg.ToBytes()
	if err != nil {
		g.log.Errorf("Failed to serialize results request message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	processed, _, results, timeout := g.broadcastAndWaitForResults(gatherBytes, eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	if processed == 0 {
		g.log.Errorf("Unexpected error waiting results for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	if timeout {
		g.log.Warningf("Could not gather all stats results for client %s and dataType %s after %d retries. Proceeding with partial results: processed %d/%d", eofMsg.ClientId, eofMsg.DataType, middleware.MAX_EOF_RETRIES, processed, eofMsg.TotalEmitted)
	}

	g.log.Infof("Received results for client %s and datatype %s: processed=%d/%d", eofMsg.ClientId, eofMsg.DataType, processed, eofMsg.TotalEmitted)

	messageToSend := results.GetMessageToSend()
	var emitted int = 0
	for _, group := range messageToSend {
		response := middleware.NewMessageWithGroupedPayload(eofMsg.DataType, eofMsg.ClientId, group, false, eofMsg.QueryId)

		middleError := g.sendMessage(*response)
		if middleError != nil {
			g.log.Errorf("problem while sending message to %s: %v", g.conf.nextStagePub, middleError)
			continue
		}
		emitted++
	}

	eofMsg.TotalEmitted = emitted
	err = g.sendMessage(eofMsg)
	if err != nil {
		g.log.Errorf("Failed to send EOF message to next stage: %v", err)
		answerMessage(NACK_DISCARD, eofMessage)
		return
	}
	answerMessage(ACK, eofMessage)
	g.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	// CLEAN
	clearMsg := middleware.NewClearResultsRequest(g.conf.id, "", eofMsg.ClientId, eofMsg.DataType)
	clearMsgBytes, err := clearMsg.ToBytes()
	if err != nil {
		g.log.Warningf("Failed to serialize results request message: %v", err)
	}
	if sendErr := g.exchangeHandlers.broadcastResultsRequestPub.Send(clearMsgBytes); sendErr != middleware.MessageMiddlewareSuccess {
		g.log.Warningf("Failed to send results request message to broadcast exchange: %v", sendErr)
	}
}

func (g *GroupByGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed, emitted int, results structures.AllowedGroup, timeout bool) {
	for retriesCount := 0; retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		results = g.conf.factory()
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := g.exchangeHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		g.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-g.resultsChans[clientId][dataType]:
				processed += msg.Processed
				emitted += msg.Emitted
				if msg.GroupedPayload != nil {
					results.AddMapString(msg.GroupedPayload)
				}
			case <-time.After(timeoutDuration):
				g.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds. Processed %d/%d", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC, processed, expectedEmitted)
				timeout = true
			}
		}
		if !timeout {
			break
		}
	}
	return processed, emitted, results, timeout
}

func (g *GroupByGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		g.log.Infof("Clearing stored group for client %s as per request from %s", msg.ClientId, msg.Origin)
		g.mutex.Lock()
		g.group.Delete(msg.ClientId)
		count, err := g.atomicWritter.CleanClient(msg.ClientId)
		if err != nil {
			return fmt.Errorf("failed to clean atomic writter for client %s: %v", msg.ClientId, err)
		}
		g.log.Infof("Cleared %d entries from atomic writter for client %s", count, msg.ClientId)
		g.getClientStats(msg.ClientId).Clear(msg.DataType)
		g.mutex.Unlock()
		answerMessage(ACK, message)
		return nil
	}

	g.mutex.Lock()
	processed, emitted := g.getClientStats(msg.ClientId).GetStats(msg.DataType)
	currentMapString := g.group.Get(msg.ClientId).ToMapString()
	g.mutex.Unlock()

	g.sendResultsRequestBatched(msg, currentMapString, BATCH_SIZE_GROUPED_MESSAGE)

	responseMsg := middleware.MessageResultsResponse{
		Origin:    g.conf.id,
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
	if sendErr := g.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	g.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) sendResultsRequestBatched(msg *middleware.MessageResultsRequest, group map[string][]string, batchSize int) error {
	for key, value := range group {
		chunks := chunkSlice(value, batchSize)
		g.log.Infof("Sending %d chunks", len(chunks))
		for _, chunk := range chunks {
			partialGroup := map[string][]string{key: chunk}
			responseMsg := middleware.MessageResultsResponse{
				Origin:         g.conf.id,
				ClientId:       msg.ClientId,
				DataType:       msg.DataType,
				GroupedPayload: partialGroup,
			}
			responseBytes, err := responseMsg.ToBytes()
			if err != nil {
				return err
			}
			if sendErr := g.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
				return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
			}
		}
	}
	return nil
}

func (g *GroupByGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	g.resultsChans[msg.ClientId][msg.DataType] <- *msg
	answerMessage(ACK, message)
	return nil
}

func chunkSlice[T any](s []T, chunkSize int) [][]T {
	if chunkSize <= 0 {
		return nil
	}

	var chunks [][]T
	for i := 0; i < len(s); i += chunkSize {
		end := i + chunkSize
		if end > len(s) {
			end = len(s)
		}
		chunks = append(chunks, s[i:end])
	}
	return chunks
}
