package main

import (
	"log"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	"encoding/json"
)


type Comment struct{
	Text string `form:"text" json:"text"`
}

func main(){

	app := fiber.New()

	api := app.Group("/api/v1")

	api.Post("/comments", createComment)

	app.Listen(":3000")

}

func ConnectProducer(brokerUrl []string) (sarama.SyncProducer, error){
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	conn, err := sarama.NewSyncProducer(brokerUrl, config)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return conn, nil
}


func PushCommentToQueue(topic string, message []byte) error{
	brokerUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokerUrl)

	if err != nil {
		log.Println(err)
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		log.Println(err)
		return err
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}


func createComment(c *fiber.Ctx) error {
	comment := new(Comment)

	if err := c.BodyParser(comment); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	cmtInBytes, err := json.Marshal(comment)
	PushCommentToQueue("comments",cmtInBytes)

	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": comment,
	})

	if err != nil {
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Failed to push comment",
		})
		return err
	}

	return err

}