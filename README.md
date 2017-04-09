# kafka-connect-elasticsearcch #
---
 * write data from Kafka into Elasticsearch index and set an index to output to that changes daily

## introdction ##
---
To run the app, compile it firstly:

    $ sh assembly/linux/test.sh

Next, start the app:

    $ cd target/linux/kafka-connect-elasticsearch-0.0.01-2017*-*-test/ && bash bin/load_kafka-connect-elasticsearch.sh monitor

If you wanna change sbin name, pls reset the value of TARGET_EXEC_NAME in assembly/common/app.properties.


## LICENCE ##
---
Apache License 2.0

