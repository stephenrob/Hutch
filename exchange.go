package hutch

import (
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"time"
)

type Exchange interface {
	MessagePublisher
	Name() string
	UpdateChannel(channel *amqp.Channel)
}

type MessagePublisher interface {
	Publish(message DeliverableMessage, key string) error
}

type exchange struct {
	name string
	ch   *amqp.Channel
}

// NewExchange registers a new AMQP exchange using the specified channel
func (b *Broker) NewExchange(name, kind string) (Exchange, error) {

	ch := b.CreateChannel()

	ex := exchange{
		name: name,
		ch:   ch,
	}

	err := ch.ExchangeDeclare(name, kind, true, false, false, false, nil)

	if err != nil {
		return nil, err
	}

	return &ex, nil
}

//Publish the deliverable message to the exchange with the given routing key
func (e *exchange) Publish(message DeliverableMessage, key string) error {
	r, err := message.MarshalRaw()

	if err != nil {
		return err
	}

	err = e.rawPublisher(r, key)

	if err != nil {
		return err
	}

	return nil
}

func (e *exchange) rawPublisher(message RawMessage, key string) error {

	msg, err := message.Encode()

	if err != nil {
		return err
	}

	err = e.ch.Publish(e.name, key, false, false, amqp.Publishing{
		Headers:     nil,
		ContentType: message.ContentType(),
		MessageId:   uuid.NewString(),
		Type:        message.Kind(),
		Timestamp:   time.Now(),
		Body:        msg,
	})

	if err != nil {
		return err
	}

	return nil
}

func (e *exchange) Name() string {
	return e.name
}

func (e *exchange) UpdateChannel(ch *amqp.Channel) {
	e.ch = ch
}

//type Publishing struct {
//	// Application or exchange specific fields,
//	// the headers exchange will inspect this field.
//	//Headers Table
//
//	// Properties
//	ContentType     string    // MIME content type - result from ContentType method on DeliverableMessage
//	ContentEncoding string    // MIME content encoding
//	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
//	Priority        uint8     // 0 to 9
//	CorrelationId   string    // correlation identifier - from context if passed in else nil
//	ReplyTo         string    // address to to reply to (ex: RPC)
//	Expiration      string    // message expiration spec
//	MessageId       string    // message identifier - generated UUID
//	Timestamp       time.Time // message timestamp - generated timestamp for publish
//	Type            string    // message type name - from message type method
//	UserId          string    // creating user id - ex: "guest"
//	AppId           string    // creating application id - from config for app id
//
//	// The application specific payload of the message
//	Body []byte // result of calling encode on message
//}
