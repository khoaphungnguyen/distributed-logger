package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func waitForRabbitMQ(url string, maxAttempts int, delay time.Duration) *amqp.Connection {
	var conn *amqp.Connection
	var err error
	for i := 0; i < maxAttempts; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			return conn
		}
		log.Printf("Waiting for RabbitMQ... (%d/%d)", i+1, maxAttempts)
		time.Sleep(delay)
	}
	log.Fatalf("Failed to connect to RabbitMQ after %d attempts: %v", maxAttempts, err)
	return nil
}

func main() {
	// 1. Connect to RabbitMQ
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}
	conn := waitForRabbitMQ(rabbitmqURL, 20, 2*time.Second)
	defer conn.Close()

	// 2. Open a channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 3. Declare a queue named "logs"
	queueName := "logs"
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 4. Start a goroutine to publish a message every second
	go func() {
		for {
			body := `{"timestamp":"` + time.Now().Format(time.RFC3339) + `","level":"INFO","message":"Hello from RabbitMQ!"}`
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = ch.PublishWithContext(ctx,
				"",        // exchange
				queueName, // routing key (queue name)
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(body),
				})
			cancel()
			if err != nil {
				log.Printf("Failed to publish a message: %v", err)
			} else {
				log.Printf("Sent: %s", body)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	// 5. Consume messages from the queue
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Println("Waiting for messages. To exit press CTRL+C")
	for msg := range msgs {
		log.Printf("Received: %s", msg.Body)
	}
}
