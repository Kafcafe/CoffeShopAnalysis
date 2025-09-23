package client

type Protocol struct {
	serverAddress string
}

func NewProtocol(serverAddress string) *Protocol {
	return &Protocol{
		serverAddress: serverAddress,
	}
}

func (p *Protocol) Connect() error {
	// Implement connection logic here
	return nil
}

func (p *Protocol) SendBatch(batch *Batch) error {
	// Implement batch sending logic here
	return nil
}

func (p *Protocol) receivedConfirmation() error {
	// Implement confirmation receiving logic here
	return nil
}

func (p *Protocol) FinishSendingFilesOf(pattern string) error {
	// Implement finish sending files logic here
	return nil
}
