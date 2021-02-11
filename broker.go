package hutch

import (
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second
)

// Broker for interfacing with RabbitMQ
type Broker struct {
	connection       *amqp.Connection
	connectionString string
	isConnected      bool
	alive            bool
	done             chan os.Signal
	notifyClose      chan *amqp.Error
	logger           *log.Logger
	channels         []*amqp.Channel
}

// NewBroker creates a new broker
func NewBroker(connectionString string, done chan os.Signal) *Broker {

	logger := log.New(os.Stdout, "BROKER: ", log.Ldate|log.Ltime|log.Lshortfile)

	broker := Broker{
		done:             done,
		alive:            true,
		logger:           logger,
		connectionString: connectionString,
	}

	go broker.handleReconnect()

	return &broker

}

func (b *Broker) handleReconnect() {

	for b.alive {
		b.isConnected = false
		t := time.Now()
		b.logger.Printf("Attempting to connect to RabbitMQ")
		var retryCount int
		for !b.connect(b.connectionString) {
			if !b.alive {
				return
			}
			select {
			case <-b.done:
				return
			case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
				b.logger.Printf("disconnected from RabbitMQ and failed to connect")
				retryCount++
			}
		}
		b.logger.Printf("Connected to RabbitMQ in: %vms", time.Since(t).Milliseconds())

		// UpdateChannel to exchanges, queues and channels

		// Wait until done or connection is closed

		select {
		case <-b.done:
			b.logger.Printf("DONE")
			return
		case <-b.notifyClose:
			b.logger.Printf("notifyClosed")
		}
	}
}

func (b *Broker) connect(connectionString string) bool {
	conn, err := amqp.Dial(connectionString)

	if err != nil {
		b.logger.Printf("failed to dial rabbitMQ server: %v", err)
		return false
	}

	b.changeConnection(conn)
	b.isConnected = true
	return true
}

func (b *Broker) changeConnection(connection *amqp.Connection) {
	b.connection = connection
	b.notifyClose = make(chan *amqp.Error)
	b.connection.NotifyClose(b.notifyClose)
}

// Close shuts down all channels for the broker connection to RabbitMQ
func (b *Broker) Close() error {
	if !b.isConnected {
		return nil
	}
	b.alive = false
	for _, ch := range b.channels {
		err := ch.Close()
		if err != nil {
			b.logger.Printf("Failed to close channel")
			return err
		}
		b.logger.Printf("Closed Channel")
	}
	err := b.connection.Close()

	if err != nil {
		b.logger.Printf("Failed to close connection")
		return err
	}

	b.isConnected = false
	b.logger.Printf("Closed connection gracefully")
	return nil
}

func (b *Broker) CreateChannel() *amqp.Channel {
	for {
		if b.isConnected {
			break
		}
		time.Sleep(1 * time.Second)
	}
	ch, err := b.connection.Channel()
	if err != nil {
		b.logger.Printf("Failed to open a channel")
		return nil
	}
	b.channels = append(b.channels, ch)
	return ch
}
