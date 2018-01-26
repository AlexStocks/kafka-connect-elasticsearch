package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/AlexStocks/goext/runtime"
	"github.com/AlexStocks/goext/time"
	"github.com/Shopify/sarama"
	sc "github.com/bsm/sarama-cluster"
)

/////////////////////////////////////////////////
// kafka messaeg -> es
/////////////////////////////////////////////////

/////////////////////////////////////////////////
// worker
/////////////////////////////////////////////////

type EsWorker struct {
	Q    chan *sarama.ConsumerMessage
	lock sync.Mutex
	done chan empty
	wg   sync.WaitGroup
}

func NewEsWorker() *EsWorker {
	return &EsWorker{
		done: make(chan empty),
	}

}

// Start for initialize all workers.
func (w *EsWorker) Start(workerNum int64, queueNum int64) {
	Log.Debug("worker number = %v, queue number is = %v", workerNum, queueNum)
	w.Q = make(chan *sarama.ConsumerMessage, queueNum)
	for i := int64(0); i < workerNum; i++ {
		w.wg.Add(1)
		go w.startEsWorker()
	}
}

var (
	workerIndex uint64
)

func (w *EsWorker) startEsWorker() {
	var (
		flag     bool
		id       int
		index    uint64
		err      error
		message  *sarama.ConsumerMessage
		docArray []interface{}
		ticker   *time.Ticker
	)

	id = gxruntime.GoID()
	index = atomic.AddUint64(&workerIndex, 1)
	Log.Info("worker{%d-%d} starts to work now.", index, id)
	ticker = time.NewTicker(gxtime.TimeSecondDuration(float64(Kafka2EsConf.Es.BulkTimeout)))
	defer ticker.Stop()

LOOP:
	for {
		select {
		case message = <-w.Q:
			Log.Debug("dequeue{worker{%d-%d} , message{topic:%v, partition:%v, offset:%v, msg:%v}}}",
				index, id, message.Topic, message.Partition, message.Offset, string(message.Value))
			KafkaLog.Info("consumer{worker{%d-%d} , message{topic:%v, partition:%v, offset:%v}}}",
				index, id, message.Topic, message.Partition, message.Offset)
			docArray = append(docArray, message.Value)
			if int(Kafka2EsConf.Es.BulkSize) <= len(docArray) {
				flag = true
			}

		// case <-time.After(gxtime.TimeSecondDuration(float64(Kafka2EsConf.Es.BulkTimeout))):
		case <-ticker.C:
			if 0 < len(docArray) {
				flag = true
			}

		case <-w.done:
			if 0 < len(docArray) {
				EsClient.BulkInsert(getIndex(), Kafka2EsConf.Es.Type, docArray)
			}
			w.wg.Done()
			Log.Info("worker{%d-%d} exits now.", index, id)
			break LOOP
		}

		if flag {
			err = EsClient.BulkInsert(getIndex(), Kafka2EsConf.Es.Type, docArray)
			if err != nil {
				Log.Error("error:%#v, log:%s", err, string(docArray[0].([]byte)))
			} else {
				Log.Info("successfully insert %d msgs into es", len(docArray))
			}
			flag = false
			docArray = docArray[:0]
		}
	}
}

func (w *EsWorker) Stop() {
	close(w.done)
	w.wg.Wait()
}

// check whether the worker has been closed.
func (w *EsWorker) IsClosed() bool {
	select {
	case <-w.done:
		return true

	default:
		return false
	}
}

func kafkaConsumerErrorCallback(err error) {
	KafkaLog.Error("kafka consumer error:%+v", err)
}

func kafkaConsumerNotificationCallback(note *sc.Notification) {
	KafkaLog.Info("kafka consumer Rebalanced: %+v", note)
}

// queueNotification add kafka message to queue list.
func (w *EsWorker) enqueueKafkaMessage(message *sarama.ConsumerMessage, preOffset int64) {
	// 	if w.IsClosed() {
	// 		return errors.New("worker has been closed!")
	// 	}
	defer KafkaConsumer.Commit(message)

	// 不重复消费kafka消息
	if preOffset != 0 && message.Offset <= preOffset {
		// 此处应对加上告警
		Log.Error("@preOffset{%d}, @message{topic:%v, partition:%v, offset:%v, msg:%v}",
			preOffset, message.Topic, message.Partition, message.Offset, string(message.Value))
		return
	}

	Log.Debug("enqueue{message:%s}", string(message.Value))
	w.Q <- message
	return
}

func (w *EsWorker) Info() string {
	return fmt.Sprintf("elasticsearch worker queue size %d", len(w.Q))
}
