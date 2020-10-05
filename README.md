**Student:** Augusto Gonzalez

**[Click here to watch project demo!!](https://web.microsoftstream.com/video/081b4c8a-ccfc-4c16-b93f-a30144329afa)**

# BDT research project on Spark Streaming
This project is part of CS523 Big Data Technologies course and its main idea is to research and get familiar with Spark Streaming and other big data technologies not covered in the lectures (I selected Kafka as recommended by the professor).
I wanted to process a stream of news articles and apply transformations on it, like filtering by key words, for example.

## Origin of data
This project uses a dataset gathered from [Newsriver](https://newsriver.io/), which is a simple API to search news articles.

## Socket Server
As we now know, Spark Streaming can consume data from a number of sources. In this project, I chose to work with a socket text stream over TCP wich seemed like a flexible approach. I created a simple socket server in Java that accepts connections on a specific port in the host machine.

## Spark Streamming
I wanted to solve each questions indiviually, so for Question 01, I have a Spark Streaming only class that receives news articles (again in JSON format) prints the headline to standard output. Of course, we can do much more than this, but it's enough for a proof of concept.

## Loading into HBase
Question 02 was about integrating Spark Streaming with either Hive or HBase (I went with HBase). Instead of writing to the console, this time I load the news articles into an HBase table with a single column family and two column descriptors ("title" and "text").
Some useful commands:
```
$ hbase shell # to start the HBase shell
hbase> scan 'news' , {COLUMNS => ['main:Title']} # to list only the Title column in the news table
```

## Kafka integration
Finally, for Question 02, I selected Kafka as the "side project" and integrated it with Spark Streaming. This is a common thing to do even in production environments. Kafka acts like a bridge between the source of data and Spark Streaming.
To use Kafka first start the server:
```
$ bin/kafka-server-start.sh config/server.properties
```
Then, create a topic (I played with a couple of arguments, like the number of partitions):
```
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic news
```
The Kafka producer will read the news article in JSON format and send them to the "news" topic. After that, a socket server will be listening at port 9999 and once a client connects (Spark will be the client), it will read from the Kafka topic and write its contents to the Spark stream for processing.
