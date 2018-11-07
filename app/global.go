package main

import (
	"fmt"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/goext/database/elasticsearch"
	"github.com/AlexStocks/goext/log"
	"github.com/AlexStocks/goext/log/kafka"
)

type (
	empty interface{}
)

var (
	// local ip
	LocalIP   string
	LocalHost string
	// progress id
	ProcessID string
	// Kafka2EsConf is main config
	Kafka2EsConf ConfYaml
	// Consumer
	KafkaConsumer gxkafka.Consumer
	// Log records server request log
	Log gxlog.Logger
	// KafkaLog records kafka message info
	KafkaLog gxlog.Logger
	// kafka message pusher worker
	Worker *EsWorker
	// es client
	EsClient gxelasticsearch.EsClient
	// for es index
	IndexLock sync.RWMutex
	TdyIndex  string
	TmwIndex  string
)

// 创建今天和明天共两天的index
func initEsIndex() {
	var (
		err error
		t   time.Time
	)

	// 创建今天的index
	t = time.Now()
	TdyIndex = Kafka2EsConf.Es.Index + fmt.Sprintf(Kafka2EsConf.Es.IndexTimeSuffixFormat, t.Year(), t.Month(), t.Day())
	err = EsClient.CreateEsIndexWithTimestamp(
		TdyIndex,
		Kafka2EsConf.Es.ShardNum,
		Kafka2EsConf.Es.ReplicaNum,
		Kafka2EsConf.Es.RefreshInterval,
		Kafka2EsConf.Es.Type,
		Kafka2EsConf.Es.KibanaTimeField,
		Kafka2EsConf.Es.KibanaTimeFormat,
	)
	if err != nil {
		panic(err)
	}
	Log.Info("create today index:%s", TdyIndex)

	// 创建第二天的index
	t = time.Now().AddDate(0, 0, 1)
	TmwIndex = Kafka2EsConf.Es.Index + fmt.Sprintf(Kafka2EsConf.Es.IndexTimeSuffixFormat, t.Year(), t.Month(), t.Day())
	err = EsClient.CreateEsIndexWithTimestamp(
		TmwIndex,
		Kafka2EsConf.Es.ShardNum,
		Kafka2EsConf.Es.ReplicaNum,
		Kafka2EsConf.Es.RefreshInterval,
		Kafka2EsConf.Es.Type,
		Kafka2EsConf.Es.KibanaTimeField,
		Kafka2EsConf.Es.KibanaTimeFormat,
	)
	if err != nil {
		panic(err)
	}
	Log.Info("create tomorrrow index:%s", TmwIndex)
}

func updateLastDate() {
	var (
		tmw   time.Time
		index string
		flag  bool
		err   error
	)

	tmw = time.Now().AddDate(0, 0, 1)
	index = Kafka2EsConf.Es.Index + fmt.Sprintf(Kafka2EsConf.Es.IndexTimeSuffixFormat, tmw.Year(), tmw.Month(), tmw.Day())

	IndexLock.RLock()
	if TmwIndex != index {
		flag = true
	}
	IndexLock.RUnlock()

	if flag {
		err = EsClient.CreateEsIndexWithTimestamp(
			index,
			Kafka2EsConf.Es.ShardNum,
			Kafka2EsConf.Es.ReplicaNum,
			Kafka2EsConf.Es.RefreshInterval,
			Kafka2EsConf.Es.Type,
			Kafka2EsConf.Es.KibanaTimeField,
			Kafka2EsConf.Es.KibanaTimeFormat,
		)
		Log.Info("CreateEsIndexWithTimestamp() = error:%#v", err)

		if err == nil {
			IndexLock.Lock()
			TdyIndex = TmwIndex
			TmwIndex = index
			IndexLock.Unlock()
		}
	}
}

func getIndex() string {
	IndexLock.RLock()
	defer IndexLock.RUnlock()
	return TdyIndex
}
