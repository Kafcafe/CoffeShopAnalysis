package join

import (
	"common/middleware"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (j *JoinGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	j.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	mh, err := middleware.NewMiddlewareHandler(j.middlewareHandler.RabbitConn)
	if err != nil {
		j.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	queueName := fmt.Sprintf("join.%s.results.request.gather.%s", j.conf.ofType, eofMsg.ClientId)
	queue, err := mh.CreateQueue(queueName)
	if err != nil {
		j.log.Errorf("Failed to create ephemeral queue %s: %v", queueName, err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	requestMsg := middleware.NewGatherResultsRequest(j.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	requestBytes, err := requestMsg.ToBytes()
	if err != nil {
		j.log.Errorf("Failed to serialize results request message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	j.ensureResultsChanExists(eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(j.processResultsResponse, j.errChan)

	j.crasher.ThrowDiceAndForceExit("eof - before requesting results")

	processed, emitted, results, timeout := j.broadcastAndWaitForResults(requestBytes, eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	if processed == 0 {
		j.log.Errorf("Unexpected error waiting results for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	if timeout {
		j.log.Warningf("Could not gather all results for client %s and dataType %s after %d retries. Proceeding with partial results: processed %d/%d, emitted %d", eofMsg.ClientId, eofMsg.DataType, middleware.MAX_EOF_RETRIES, processed, eofMsg.TotalEmitted, emitted)
	}
	delete(j.resultsChans[eofMsg.ClientId], eofMsg.DataType)
	delete(j.resultsChans, eofMsg.ClientId)
	if err := queue.StopConsuming(); err != middleware.MessageMiddlewareSuccess {
		j.log.Warningf("Failed to stop consuming ephemeral queue %s: %v", queueName, err)
	}
	if err := queue.Delete(); err != middleware.MessageMiddlewareSuccess {
		j.log.Warningf("Failed to delete ephemeral queue %s: %v", queueName, err)
	}

	j.crasher.ThrowDiceAndForceExit("eof - after requesting results - before sending next stage")

	response := middleware.NewMessageWithPayload(eofMsg.DataType, eofMsg.ClientId, results, false, eofMsg.QueryId)

	if middleError := j.sendNextStage(*response); middleError != nil {
		j.log.Errorf("problem while sending message to %s: %v", j.conf.nextStagePubs[j.conf.ofType], middleError)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	emitted++

	expectedTotal := eofMsg.TotalEmitted
	eofMsg.TotalEmitted = emitted
	if err := j.sendNextStage(eofMsg); err != nil {
		j.log.Errorf("Failed to send EOF message to next stage: %v. Requeuing message...", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	j.crasher.ThrowDiceAndForceExit("eof - after sending next stage - before ack")

	j.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Processed count: %d/%d, Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, processed, expectedTotal, emitted)
	answerMessage(ACK, eofMessage)

	clearMsg := middleware.NewClearResultsRequest(j.conf.id, "", eofMsg.ClientId, eofMsg.DataType)
	clearMsgBytes, err := clearMsg.ToBytes()
	if err != nil {
		j.log.Warningf("Failed to serialize results request message: %v", err)
	}
	if sendErr := j.middlewareHandlers.broadcastResultsRequestPub.Send(clearMsgBytes); sendErr != middleware.MessageMiddlewareSuccess {
		j.log.Warningf("Failed to send results request message to broadcast exchange: %v", sendErr)
	}
}

func (j *JoinGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		j.getClientStats(msg.ClientId).Clear(msg.DataType)
		delete(j.sideTable, msg.ClientId)
		delete(j.mainTable, msg.ClientId)
		delete(j.sideTableReceived, msg.ClientId)
		total, err := j.atomicWritter.CleanClient(msg.ClientId)
		if err != nil {
			j.log.Warningf("Failed to count lines for client %s and datatype %s: %v", msg.ClientId, msg.DataType, err)
		}
		j.log.Infof("Cleared stored data for client %s and datatype %s. Total lines removed: %d", msg.ClientId, msg.DataType, total)
		j.log.Infof("Cleared stats for client %s and datatype %s", msg.ClientId, msg.DataType)
		answerMessage(ACK, message)
		return nil
	}

	j.log.Infof("Received results request message from %s for client %s and datatype %s", msg.Origin, msg.ClientId, msg.DataType)
	processed, emitted, _, _ := j.getClientStats(msg.ClientId).GetStats(msg.DataType)

	responseMsg := middleware.MessageResultsResponse{
		Origin:    j.conf.id,
		ClientId:  msg.ClientId,
		DataType:  msg.DataType,
		Processed: processed,
		Emitted:   emitted,
	}

	if j.conf.ofType == JOIN_USERS_TYPE {
		j.mutex.Lock()
		currentResults, exists := j.sideTable[msg.ClientId]
		j.mutex.Unlock()
		if !exists {
			currentResults = []string{}
		}
		responseMsg.Payload = j.parseOnlyAlreadyJoinedLines(currentResults)
	} else {
		j.mutex.Lock()
		currentResults, exists := j.mainTable[msg.ClientId]
		sideTable := j.sideTable[msg.ClientId]
		j.mutex.Unlock()
		if !exists {
			currentResults = []string{}
		}
		responseMsg.Payload = j.conf.joinTables(NewJoiner(), sideTable, currentResults)
	}

	j.crasher.ThrowDiceAndForceExit("gather results - while sending results")

	responseBytes, err := responseMsg.ToBytes()
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if sendErr := j.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	j.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed, emitted int, results []string, timeout bool) {
	for retriesCount := 0; retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		results = []string{}
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := j.middlewareHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			j.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		j.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-j.resultsChans[clientId][dataType]:
				processed += msg.Processed
				emitted += msg.Emitted
				if msg.Payload != nil {
					results = append(results, msg.Payload...)
				}
			case <-time.After(timeoutDuration):
				j.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds. Processed %d/%d", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC, processed, expectedEmitted)
				timeout = true
			}
		}
		if !timeout {
			break
		}
	}
	return processed, emitted, results, timeout
}

func (j *JoinGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	j.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
	j.resultsChans[msg.ClientId][msg.DataType] <- *msg
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := j.resultsChans[clientId]; !exists {
		j.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := j.resultsChans[clientId][dataType]; !exists {
		j.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (j *JoinGenericWorker) parseOnlyAlreadyJoinedLines(lines []string) []string {
	result := []string{}

	for _, line := range lines {
		splitted := strings.SplitN(line, ",", 2)
		if strings.Contains(splitted[1], "-") {
			result = append(result, line)
		}
	}

	return result
}
