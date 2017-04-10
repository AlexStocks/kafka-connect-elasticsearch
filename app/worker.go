package main

import (
	"sync"
	"sync/atomic"
)

import (
	"github.com/AlexStocks/goext/runtime"
	"github.com/Shopify/sarama"
	sc "github.com/bsm/sarama-cluster"
)

/////////////////////////////////////////////////
// kafka messaeg -> es
/////////////////////////////////////////////////

/////////////////////////////////////////////////
// pusher-worker
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
		id      int
		index   uint64
		err     error
		message *sarama.ConsumerMessage
	)

	id = gxruntime.GoID()
	index = atomic.AddUint64(&workerIndex, 1)
	Log.Info("worker{%d-%d} starts to work now.", index, id)

LOOP:
	for {
		select {
		case message = <-w.Q:
			Log.Debug("dequeue{worker{%d-%d} , message{topic:%v, partition:%v, offset:%v, msg:%v}}}",
				index, id, message.Topic, message.Partition, message.Offset, string(message.Value))
			err = EsClient.Insert(getIndex(), Kafka2EsConf.Es.Type, message.Value)
			if err != nil {
				Log.Error("%#v", err)
			} else {
				Log.Info("successfully insert msg %#v into es", (string)(message.Value))
			}

		case <-w.done:
			w.wg.Done()
			Log.Info("worker{%d-%d} exits now.", index, id)
			break LOOP
		}
	}
}

func (w *EsWorker) Stop() {
	close(w.done)
	w.wg.Wait()
}

// check whether the session has been closed.
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
