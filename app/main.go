package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

import (
	"github.com/AlexStocks/goext/log"
	"github.com/AlexStocks/goext/log/elasticsearch"
	"github.com/AlexStocks/goext/log/kafka"
	"github.com/AlexStocks/goext/net"
	"github.com/AlexStocks/goext/time"
)

const (
	APP_CONF_FILE           string = "APP_CONF_FILE"
	APP_LOG_CONF_FILE       string = "APP_LOG_CONF_FILE"
	APP_KAFKA_LOG_CONF_FILE string = "APP_KAFKA_LOG_CONF_FILE"
)

const (
	FAILFAST_TIMEOUT = 3 // in second
)

var (
	pprofPath = "/debug/pprof/"

	usageStr = `
Usage: kafka-connect-elasticsearch [options]
Server Options:
    -c, --config <file>              Configuration file path
    -k, --kafka_log <file>           Kafka Log configuration file
    -l, --log <file>                 Log configuration file
Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
`
)

// usage will print out the flag options for the server.
func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func getHostInfo() {
	var (
		err error
	)

	LocalHost, err = os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("os.Hostname() = %s", err))
	}

	LocalIP, err = gxnet.GetLocalIP(LocalIP)
	if err != nil {
		panic("can not get local IP!")
	}

	ProcessID = fmt.Sprintf("%s@%s", LocalIP, LocalHost)
}

func createPIDFile() error {
	if !Kafka2EsConf.Core.PID.Enabled {
		return nil
	}

	pidPath := Kafka2EsConf.Core.PID.Path
	_, err := os.Stat(pidPath)
	if os.IsNotExist(err) || Kafka2EsConf.Core.PID.Override {
		currentPid := os.Getpid()
		if err := os.MkdirAll(filepath.Dir(pidPath), os.ModePerm); err != nil {
			return fmt.Errorf("Can't create PID folder on %v", err)
		}

		file, err := os.Create(pidPath)
		if err != nil {
			return fmt.Errorf("Can't create PID file: %v", err)
		}
		defer file.Close()
		if _, err := file.WriteString(strconv.FormatInt(int64(currentPid), 10)); err != nil {
			return fmt.Errorf("Can'write PID information on %s: %v", pidPath, err)
		}
	} else {
		return fmt.Errorf("%s already exists", pidPath)
	}
	return nil
}

// initLog use for initial log module
func initLog(logConf string) {
	Log = gxlog.NewLoggerWithConfFile(logConf)
}

// initLog use for initial log module
func initKafkaLog(logConf string) {
	KafkaLog = gxlog.NewLoggerWithConfFile(logConf)
}

// initEsClient initialise EsClient
func initEsClient() {
	var (
		err error
	)

	// Create a client
	EsClient, err = gxelasticsearch.CreateEsClient(Kafka2EsConf.Es.EsHosts)
	if err != nil {
		panic(err)
	}

	initEsIndex()
}

func initWorker() {
	Worker = NewEsWorker()
	Worker.Start(int64(Kafka2EsConf.Core.WorkerNum), int64(Kafka2EsConf.Core.QueueNum))
}

func initKafkaConsumer() {
	var (
		err error
		id  string
	)

	id = LocalIP + "-" + LocalHost + "-" + "kafka2es"
	KafkaConsumer, err = gxkafka.NewConsumer(
		id,
		strings.Split(Kafka2EsConf.Kafka.Brokers, ","),
		[]string{Kafka2EsConf.Kafka.Topic},
		Kafka2EsConf.Kafka.ConsumerGroup,
		Worker.enqueueKafkaMessage,
		kafkaConsumerErrorCallback,
		kafkaConsumerNotificationCallback,
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize Kafka consumer: %v", err))
	}

	err = KafkaConsumer.Start()
	if err != nil {
		panic(fmt.Sprintf("Failed to start Kafka consumer: %v", err))
	}
}

func initSignal() {
	// signal.Notify的ch信道是阻塞的(signal.Notify不会阻塞发送信号), 需要设置缓冲
	signals := make(chan os.Signal, 1)
	// It is not possible to block SIGKILL or syscall.SIGSTOP
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case sig := <-signals:
			Log.Info("get signal %s", sig.String())
			switch sig {
			case syscall.SIGHUP:
			// reload()
			default:
				go gxtime.Future(Kafka2EsConf.Core.FailFastTimeout, func() {
					Log.Warn("app exit now by force...")
					os.Exit(1)
				})

				// 要么survialTimeout时间内执行完毕下面的逻辑然后程序退出，要么执行上面的超时函数程序强行退出
				KafkaConsumer.Stop()
				KafkaLog.Close()
				Log.Warn("app exit now...")
				Log.Close()
				return
			}
		case <-time.After(time.Duration(60e9)):
			updateLastDate()
		}
	}
}

func main() {
	var (
		err          error
		showVersion  bool
		configFile   string
		logConf      string
		kafkaLogConf string
	)

	/////////////////////////////////////////////////
	// conf
	/////////////////////////////////////////////////

	SetVersion(Version)

	flag.BoolVar(&showVersion, "v", false, "Print version information.")
	flag.BoolVar(&showVersion, "version", false, "Print version information.")
	flag.StringVar(&configFile, "c", "", "Configuration file path.")
	flag.StringVar(&configFile, "config", "", "Configuration file path.")
	flag.StringVar(&logConf, "l", "", "Logger configuration file.")
	flag.StringVar(&logConf, "log", "", "Logger configuration file.")
	flag.StringVar(&kafkaLogConf, "k", "", "Kafka logger configuration file.")
	flag.StringVar(&kafkaLogConf, "kafka_log", "", "Kafka logger configuration file.")

	flag.Usage = usage
	flag.Parse()

	// Show version and exit
	if showVersion {
		PrintVersion()
		os.Exit(0)
	}

	if configFile == "" {
		configFile = os.Getenv(APP_CONF_FILE)
		if configFile == "" {
			usage()
		}
	}
	if path.Ext(configFile) != ".yml" {
		panic(fmt.Sprintf("application configure file name{%v} suffix must be .yml", configFile))
	}
	Kafka2EsConf, err = LoadConfYaml(configFile)
	if err != nil {
		log.Printf("Load yaml config file error: '%v'", err)
		return
	}
	fmt.Printf("config: %+v\n", Kafka2EsConf)

	if logConf == "" {
		logConf = os.Getenv(APP_LOG_CONF_FILE)
		if configFile == "" {
			usage()
		}
	}

	if kafkaLogConf == "" {
		kafkaLogConf = os.Getenv(APP_KAFKA_LOG_CONF_FILE)
		if kafkaLogConf == "" {
			usage()
		}
	}

	/////////////////////////////////////////////////
	// worker
	/////////////////////////////////////////////////
	if Kafka2EsConf.Core.FailFastTimeout == 0 {
		Kafka2EsConf.Core.FailFastTimeout = FAILFAST_TIMEOUT
	}

	getHostInfo()

	initLog(logConf)
	initKafkaLog(kafkaLogConf)

	if err = createPIDFile(); err != nil {
		Log.Critic(err)
	}

	initEsClient()
	initWorker()
	// kafka message receiver
	initKafkaConsumer()

	initSignal()
}
