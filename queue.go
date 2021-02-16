package hutch

import (
	"context"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

type Queue interface {
	Subscribe(cancelCtx context.Context, handlerFunc MessageHandlerFunc) error
	Bind(Exchange, string) error
	WG() *sync.WaitGroup
}

type queue struct {
	name string
	ch   *amqp.Channel
	q    *amqp.Queue
	wg   *sync.WaitGroup
}

// NewQueue registers a new AMQP queue using the specified channel
func (b *Broker) NewQueue(name string) (Queue, error) {

	ch := b.CreateChannel()

	qd, err := ch.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	q := queue{
		name: name,
		ch:   ch,
		q:    &qd,
		wg:   &sync.WaitGroup{},
	}

	return &q, nil
}

func (q queue) Bind(ex Exchange, key string) error {
	err := q.ch.QueueBind(q.name, key, ex.Name(), false, nil)
	if err != nil {
		return err
	}
	return nil
}

type MessageHandlerFunc func(m RawMessage)

func (q *queue) WG() *sync.WaitGroup {
	return q.wg
}

func (q *queue) Subscribe(cancelCtx context.Context, handlerFunc MessageHandlerFunc) error {

	err := q.ch.Qos(1, 0, false)
	if err != nil {
		return err
	}

	var connectionDropped bool
	threads := 2

	for i := 1; i <= threads; i++ {
		msgs, err := q.ch.Consume(q.name, fmt.Sprintf("%s-consumer-%d", q.name, i), true, false, false, false, nil)
		if err != nil {
			return err
		}

		q.wg.Add(1)

		go func() {
			defer q.wg.Done()
			for {
				select {
				case <-cancelCtx.Done():
					return
				case msg, ok := <-msgs:
					if !ok {
						connectionDropped = true
						return
					}
					raw := &rawMessage{
						raw:  msg.Body,
						id:   msg.MessageId,
						kind: msg.Type,
					}
					err := RawUnmarshal(msg.Body, raw)
					if err != nil {
						continue
					}
					handlerFunc(raw)
				}
			}
		}()
	}

	if connectionDropped {
		return nil
	}

	return nil
}
