package levelq

// Adapted from 
// https://github.com/beeker1121/goque/blob/4044bc29b28064db4f08e947c4972d5ca3e0f3c8/queue.go
// under the MIT License @ 
// https://github.com/beeker1121/goque/blob/4044bc29b28064db4f08e947c4972d5ca3e0f3c8/LICENSE.

import (
	"bytes"
	"math"
	"encoding/gob"
	"encoding/binary"
	"os"
	"sync"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
)

// ErrOutOfBounds is returned when the ID used to lookup an item
// is outside of the range of the queue.
var ErrOutOfBounds = errors.New("levelq: ID used is outside range of stack or queue")

// ErrDBClosed is returned when the Close function has already
// been called, causing the queue to close, as well as
// its underlying database.
var ErrDBClosed = errors.New("levelq: Database is closed")

// ErrEmpty is returned when the queue is empty.
var ErrEmpty = errors.New("levelq: Queue is empty")

// ErrFull is returned when the queue is full. 
// This error should almost never be thrown.
var ErrFull = errors.New("levelq: Queue is full")

// MaxQueueSize is the largest the Queue can grow to
// The value math.MaxUint64 >> 2 is used in order 
// to prevent overflow when the size calculation is performed
// i.e. (MaxQueueSize + tail - head) % MaxQueueSize
var MaxQueueSize uint64 = math.MaxUint64 >> 2

// Queue is a standard FIFO (first in, first out) queue.
type Queue struct {
	sync.RWMutex
	DataDir string
	db      *leveldb.DB
	head    uint64
	tail    uint64
	isOpen  bool
}

// OpenQueue opens a queue if one exists at the given directory. If one
// does not already exist, a new queue is created.
func OpenQueue(dataDir string) (*Queue, error) {
	var err error

	// Create a new Queue.
	q := &Queue{
		DataDir: dataDir,
		db:      &leveldb.DB{},
		head:    0,
		tail:    0,
		isOpen:  false,
	}

	// Open database for the queue.
	q.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return q, err
	}

	// Set isOpen and return.
	q.isOpen = true
	return q, q.init()
}

// idToKey converts and returns the given ID to a key.
func idToKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

// keyToID converts and returns the given key to an ID.
func keyToID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

// enqueue adds a value to the queue.
func (q *Queue) enqueue(value []byte) error {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return ErrDBClosed
	}

	// We explicitly wrap the integer index around
	// so that way if Go changes the overflow behavior, 
	// this code will still work.
	nextID := (q.tail + 1) % MaxQueueSize

	if nextID == q.head {
		return ErrFull
	}

	key := idToKey(nextID)

	// Add it to the queue.
	if err := q.db.Put(key, value, nil); err != nil {
		return err
	}

	// Increment tail position.
	q.tail = nextID

	return nil
}


// EnqueueObject is a helper function for Enqueue that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
func (q *Queue) EnqueueObject(value interface{}) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value); err != nil {
		return err
	}
	return q.enqueue(buffer.Bytes())
}

// dequeue removes the next value in the queue and returns it.
func (q *Queue) dequeue() ([]byte, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// We explicitly wrap the integer index around
	// so that way if Go changes the overflow behavior, 
	// this code will still work.
	nextID := (q.head + 1) % MaxQueueSize

	// Try to get the next value in the queue.
	value, err := q.getValueByID(nextID)
	if err != nil {
		return nil, err
	}

	key := idToKey(nextID)

	// Remove this value from the queue.
	if err := q.db.Delete(key, nil); err != nil {
		return nil, err
	}

	// Increment head position.
	q.head = nextID

	return value, nil
}

// DequeueObject removes the next value in the queue and deserializes
// the gob encoded value.
//
// The value passed to this method should be a pointer to a variable
// of the type you wish to decode into. The variable pointed to will
// hold the decoded object. 
func (q *Queue) DequeueObject(valueIface interface{}) error {
	value, err := q.dequeue()
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(bytes.NewReader(value))
	if err := dec.Decode(valueIface); err != nil {
		return err
	}
	return nil
}


// Length returns the total number of values in the queue.
func (q *Queue) Length() uint64 {
	return (MaxQueueSize + q.tail - q.head) % MaxQueueSize
}

// Close closes the LevelDB database of the queue.
func (q *Queue) Close() error {
	q.Lock()
	defer q.Unlock()

	// Check if queue is already closed.
	if !q.isOpen {
		return nil
	}

	// Close the LevelDB database.
	if err := q.db.Close(); err != nil {
		return err
	}

	// Reset queue head and tail and set
	// isOpen to false.
	q.head = 0
	q.tail = 0
	q.isOpen = false

	return nil
}

// Drop closes and deletes the LevelDB database of the queue.
func (q *Queue) Drop() error {
	if err := q.Close(); err != nil {
		return err
	}

	return os.RemoveAll(q.DataDir)
}

// getValueByID returns an value, if found, for the given ID.
func (q *Queue) getValueByID(id uint64) ([]byte, error) {
	// Check if empty or out of bounds.
	if q.Length() == 0 {
		return nil, ErrEmpty
	} else if q.head < q.tail {
		// in this case neither head nor tail has
		// wrapped around
		if id <= q.head || id > q.tail {
			return nil, ErrOutOfBounds
		}
	} else if q.tail < q.head {
		// in this case tail has wrapped around
		// but head has not
		if id > q.tail && id <= q.head {
			return nil, ErrOutOfBounds
		}
	}

	// Get value from database.
	key := idToKey(id)
	var value []byte
	var err error
	if value, err = q.db.Get(key, nil); err != nil {
		return nil, err
	}

	return value, nil
}

// init initializes the queue data.
func (q *Queue) init() error {
	// Create a new LevelDB Iterator.
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	// Set queue head to the first id
	if iter.First() {
		q.head = keyToID(iter.Key()) - 1
	}

	// Set queue tail to the last id
	if iter.Last() {
		q.tail = keyToID(iter.Key())
	}

	return iter.Error()
}
