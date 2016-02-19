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

------------------------------------------------------------------------------------------
http://hmkcode.com/android-google-cloud-messaging-tutorial/

