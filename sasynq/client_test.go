package sasynq

import (
	"log"
	"testing"
	"time"
)

func runProducer(client *Client) error {
	userPayload1 := &EmailPayload{UserID: 101, Message: "Critical Update"}
	_, info, err := client.EnqueueNow(TypeEmailSend, userPayload1, WithQueue("critical"), WithRetry(5))
	if err != nil {
		return err
	}
	log.Printf("enqueued task: type=%s, id=%s queue=%s", TypeEmailSend, info.ID, info.Queue)

	userPayload2 := &SMSPayload{UserID: 202, Message: "Weekly Newsletter"}
	_, info, err = client.EnqueueIn(time.Second*5, TypeSMSSend, userPayload2, WithQueue("default"), WithRetry(3))
	if err != nil {
		return err
	}
	log.Printf("enqueued task: type=%s, id=%s queue=%s", TypeSMSSend, info.ID, info.Queue)

	userPayload3 := &MsgNotificationPayload{UserID: 303, Message: "Promotional Offer"}
	_, info, err = client.EnqueueAt(time.Now().Add(time.Second*10), TypeMsgNotification, userPayload3, WithQueue("low"), WithRetry(1))
	if err != nil {
		return err
	}
	log.Printf("enqueued task: type=%s, id=%s queue=%s", TypeMsgNotification, info.ID, info.Queue)

	// Equivalent EnqueueNow function
	userPayload4 := &EmailPayload{UserID: 404, Message: "Important Notification"}
	task, err := NewTask(TypeEmailSend, userPayload4)
	if err != nil {
		return err
	}
	info, err = client.Enqueue(task, WithQueue("low"), WithRetry(1), WithDeadline(time.Now().Add(time.Second*15)), WithUniqueID("custom-id-xxxx-xxxx"))
	if err != nil {
		return err
	}
	log.Printf("enqueued task: type=%s, id=%s queue=%s", TypeEmailSend, info.ID, info.Queue)

	return nil
}

func TestNewClient(t *testing.T) {
	client := NewClient(getRedisConfig())

	err := runProducer(client)
	if err != nil {
		t.Log("run producer failed:", err)
		return
	}
	defer client.Close()

	log.Println("all tasks enqueued")
}
