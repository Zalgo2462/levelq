package levelq

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestQueueClose(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	if err = q.EnqueueObject("value"); err != nil {
		t.Error(err)
	}

	if q.Length() != 1 {
		t.Errorf("Expected queue length of 1, got %d", q.Length())
	}

	q.Close()

	var testStr string

	if err = q.DequeueObject(&testStr); err != ErrDBClosed {
		t.Errorf("Expected to get database closed error, got %s", err.Error())
	}

	if q.Length() != 0 {
		t.Errorf("Expected queue length of 0, got %d", q.Length())
	}
}


func TestQueueDrop(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}

	if _, err = os.Stat(file); os.IsNotExist(err) {
		t.Error(err)
	}

	q.Drop()

	if _, err = os.Stat(file); err == nil {
		t.Error("Expected directory for test database to have been deleted")
	}
}

func TestQueueEnqueue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		if err = q.EnqueueObject(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue size of 10, got %d", q.Length())
	}
}

func TestQueueDequeue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		if err = q.EnqueueObject(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue length of 10, got %d", q.Length())
	}

	var deqItem string

	err = q.DequeueObject(&deqItem)

	if err != nil {
		t.Error(err)
	}

	if q.Length() != 9 {
		t.Errorf("Expected queue length of 9, got %d", q.Length())
	}

	compStr := "value for item 1"

	if deqItem != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, deqItem)
	}
}

func TestQueueEmpty(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	err = q.EnqueueObject("value for item")
	if err != nil {
		t.Error(err)
	}

	var deqItem string
	err = q.DequeueObject(&deqItem)
	if err != nil {
		t.Error(err)
	}

	err = q.DequeueObject(&deqItem)
	if err != ErrEmpty {
		t.Errorf("Expected to get empty error, got %s", err.Error())
	}
}

func TestQueueWrap(t *testing.T) {
	//Hack MaxQueueSize so we aren't waiting forever
	oldMaxQueueSize := MaxQueueSize
	MaxQueueSize = 5
	defer func() {
		MaxQueueSize = oldMaxQueueSize
	}()

	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := uint64(0); i < MaxQueueSize - 1; i++ {
		var deq bool
		err = q.EnqueueObject(true)
		if err != nil {
			t.Error(err)
		}
		err = q.DequeueObject(&deq)
		if err != nil {
			t.Error(err)
		}
	}

	// Enqueue an object in the last slot
	err = q.EnqueueObject(true)
	if err != nil {
		t.Error(err)
	}

	// Test Length when head = MaxQueueSize - 1 and tail = 0
	if q.Length() != 1 {
		t.Errorf("Queue failed to wrap properly. Expected length == 1. Found %d", q.Length())
	}

	var deq bool

	err = q.DequeueObject(&deq)
	if err != nil {
		t.Error(err)
	}

	// Ensure MaxQueueSize Enqueues and Dequeues gets back to where we started
	if q.head != 0 || q.tail != 0 {
		t.Errorf("Queue failed to wrap properly. Expected (head, tail) == (0, 0). Found (%d, %d)", q.head, q.tail)
	}
}


