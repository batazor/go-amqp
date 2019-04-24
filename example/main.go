package main

import (
	"fmt"
	wrapper "github.com/batazor/go-amqp/pkg/amqp"
)

const (
	AMQP_URI           = "amqp://guest:guest@localhost:5672/"
	AMQP_EXCHANGE_LIST = "demo1,demo2"
	AMQP_EXCHANGE_TYPE = "headers"
	AMQP_NAME_QUEUE    = "test"
)

func main() {
	CONSUMER := wrapper.NewConsumer(
		AMQP_URI,
		AMQP_EXCHANGE_LIST,
		AMQP_EXCHANGE_TYPE,
		AMQP_NAME_QUEUE,
		"",
		"",
	)

	err := CONSUMER.Connect()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Run AMQP")

	cfg := wrapper.AnnounceQueue{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Arguments:  nil,
	}

	err = CONSUMER.AnnounceQueue(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	go CONSUMER.Handle(handler, AMQP_NAME_QUEUE)
	select {}
}

func handler(deliveries wrapper.Delivery) {
	for i := 0; i < 1; i++ {
		go func() {

			for d := range deliveries {
				fmt.Println(d.Body)

				d.Ack(false)
			}
		}()
	}
}
