package hutch

import (
	"encoding"
	"os"
)

type Client interface {
	Close() error
	NewTopicExchange(string) (Exchange, error)
	NewQueue(string) (Queue, error)
}

// Client holds all information for a hutch broker connection
type client struct {
	broker *Broker
	config *config
}

type config struct {
	AppID     string
	Marshaler encoding.TextMarshaler
}

// NewClient initialises a new hutch client
func NewClient(connectionString string, done chan os.Signal) (Client, error) {
	b := NewBroker(connectionString, done)
	c := client{
		broker: b,
	}

	return &c, nil
}

// Close closes down a client connection to the hutch broker
func (c *client) Close() error {
	return c.broker.Close()
}

func (c *client) NewTopicExchange(name string) (Exchange, error) {
	ex, err := c.broker.NewExchange(name, "topic")
	if err != nil {
		return nil, err
	}
	return ex, nil
}

func (c *client) NewQueue(name string) (Queue, error) {
	q, err := c.broker.NewQueue(name)
	if err != nil {
		return nil, err
	}
	return q, nil
}