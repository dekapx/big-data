kafka-storm-demo
===================

# start zookeeper (Linux/Windows)
bin/zookeeper-server-start.sh config/zookeeper.properties
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# start kafka broker (Linux/Windows)
bin/kafka-server-start.sh config/server.properties
bin\windows\kafka-server-start.bat config\server.properties

# create a topic (Linux/Windows)
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic
bin\windows\kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic

# list the topics (Linux/Windows)
bin/kafka-topics.sh --list --zookeeper localhost:2181
bin\windows\kafka-topics --list --zookeeper localhost:2181

# send message to topic (Linux/Windows)
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic kafkatopic
bin\windows\kafka-console-producer.sh --broker-list localhost:9092 --topic kafkatopic
+-----------------------------------------------------------------------------------------------------------------+
# configure kafka
- zookeeper.properties (set data directory path)
	- dataDir=C:/Installers/kafka/zookeeper-data
- server.properties	(set log directory path)
	- log.dirs=C:/Installers/kafka/kafka-logs
+-----------------------------------------------------------------------------------------------------------------+
# start zookeeper
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# start kafka broker
$ bin/kafka-server-start.sh config/server.properties
$ bin\windows\kafka-server-start.bat config\server.properties

# create a topic
$ kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafkatopic
$ bin\windows\kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafkatopic

# list the topics
$ bin/kafka-topics.sh --list --zookeeper localhost:2181
$ bin\windows\kafka-topics --list --zookeeper localhost:2181

# send message to topic
$ bin/kafka-console-producer.sh --broker-list localhost:2181 --topic kafkatopic
$ bin\windows\kafka-console-producer --broker-list localhost:2181 --topic kafkatopic
+-----------------------------------------------------------------------------------------------------------------+
http://kafka.apache.org/07/quickstart.html
http://wpcertification.blogspot.ie/2014/08/java-client-for-publishing-and.html
https://apache.googlesource.com/kafka/+/0.8.2.0/examples/src/main/java/kafka/examples/SimpleConsumerDemo.java
https://github.com/kafka-dev/kafka/tree/master/examples/src/main/java/kafka/examples
http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
http://blog.cloudera.com/blog/2014/09/apache-kafka-for-beginners/
+-----------------------------------------------------------------------------------------------------------------+
http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html
+-----------------------------------------------------------------------------------------------------------------+
http://hmkcode.com/android-google-cloud-messaging-tutorial/

