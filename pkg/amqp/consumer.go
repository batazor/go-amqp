package amqp

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"runtime"
	"strings"
	"time"
)

func NewConsumer(uri, changes, exchangeType, queueName, bindingKey, consumerTag string) *Consumer {
	return &Consumer{
		uri:          uri,
		queueName:    queueName,
		changes:      changes,
		bindingKey:   bindingKey,
		exchangeType: exchangeType,
		conn:         nil,
		channel:      nil,
		consumerTag:  consumerTag,
		done:         make(chan error),
	}

}

func (c *Consumer) Connect() error {
	var err error

	c.conn, err = amqp.Dial(c.uri)
	if err != nil {
		return errors.New("Failed to connect to RabbitMQ: " + err.Error())
	}

	c.conn.Config.ChannelMax = 300

	go func() {
		for {
			select {
			case notify := <-c.conn.NotifyClose(make(chan *amqp.Error)):
				// Waits here for the channel to be closed
				log.Print("Notify close: ", notify)

				// Let Handle know it's not time to reconnect
				c.done <- errors.New("Channel Closed")
			}
		}
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return errors.New("Failed to open a channel: " + err.Error())
	}

	err = c.CreateExchange(c.changes, "")
	if err != nil {
		return errors.New("Failed to declare the Exchange: " + err.Error())
	}

	return nil
}

// AnnounceQueue sets the queue that will be listened to for this
// connection...
func (c *Consumer) AnnounceQueue(cfg AnnounceQueue) error {
	c.cfgAnnounceQueue = cfg

	var err error
	c.queue, err = c.channel.QueueDeclare(
		c.queueName,    // name of the queue
		cfg.Durable,    // durable
		cfg.AutoDelete, // delete when usused
		cfg.Exclusive,  // exclusive
		cfg.NoWait,     // noWait
		cfg.Arguments,  // arguments
	)
	if err != nil {
		return errors.New("Failed to declare a queue: " + err.Error())
	}

	// Qos determines the amount of messages that the queue will pass to you before
	// it waits for you to ack them. This will slow down queue consumption but
	// give you more certainty that all messages are being processed. As load increases
	// I would recommend upping the about of Threads and Processors the go process
	// uses before changing this although you will eventually need to reach some
	// balance between threads, procs, and Qos.
	err = c.channel.Qos(500, 0, false)
	if err != nil {
		return errors.New("Error setting qos: " + err.Error())
	}

	exchangeList := strings.Split(c.changes, ",")
	for _, echangeName := range exchangeList {
		name := strings.Trim(echangeName, " ")
		err = c.channel.QueueBind(
			c.queue.Name, // name of the queue
			c.bindingKey, // bindingKey
			name,         // sourceExchange
			false,        // noWait
			nil,          // arguments
		)
		if err != nil {
			return errors.New("Failed to bind a queue: " + err.Error())
		}
	}

	c.Delivery, err = c.channel.Consume(
		c.queue.Name,  // name
		c.consumerTag, // consumerTag,
		false,         // noAck
		false,         // exclusive
		false,         // noLocal
		false,         // noWait
		nil,           // arguments
	)
	if err != nil {
		return errors.New("Failed to register a consumer: " + err.Error())
	}

	return nil
}

// Reconnect is called in places where NotifyClose() channel is called
// wait 30 seconds before trying to reconnect. Any shorter amount of time
// will  likely destroy the error log while waiting for servers to come
// back online. This requires two parameters which is just to satisfy
// the AccounceQueue call and allows greater flexability
func (c *Consumer) Reconnect(queueName string) error {
	time.Sleep(10 * time.Second)

	if err := c.Connect(); err != nil {
		return errors.New("Could not connect in reconnect call: " + err.Error())
	}

	err := c.AnnounceQueue(c.cfgAnnounceQueue)
	if err != nil {
		return errors.New("Couldn't connect")
	}

	return nil
}

// Handle has all the logic to make sure your program keeps running
// d should be a delievey channel as created when you call AnnounceQueue
// fn should be a function that handles the processing of deliveries
// this should be the last thing called in main as code under it will
// become unreachable unless put int a goroutine. The q and rk params
// are redundant but allow you to have multiple queue listeners in main
// without them you would be tied into only using one queue per connection
func (c *Consumer) Handle(fn func(Delivery), queue string) {
	threads := MaxParallelism()

	for {
		for i := 0; i < threads; i++ {
			go fn(c.Delivery)
		}

		// Go into reconnect loop when
		// c.done is passed non nil values
		if <-c.done != nil {
			err := c.Reconnect(queue)
			if err != nil {
				// Very likely chance of failing
				// should not cause worker to terminate
				log.Print("Reconnecting Error ", err)
			}

			log.Print("Reconnected... possibly")
		}
	}
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Close(); err != nil {
		return err
	}

	if err := c.conn.Close(); err != nil {
		return err
	}

	defer log.Print("AMQP shutwodn OK")

	// wait for handle() to exit
	return <-c.done
}

func (c *Consumer) CreateExchange(exchanges string, exchangeType string) error {
	if exchangeType == "" {
		exchangeType = c.exchangeType
	}

	exchangeList := strings.Split(exchanges, ",")
	for _, echangeName := range exchangeList {
		name := strings.Trim(echangeName, " ")
		err := c.channel.ExchangeDeclare(
			name,
			exchangeType,
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return errors.New("Failed to declare the Exchange: " + err.Error())
		}
	}

	return nil
}

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}
