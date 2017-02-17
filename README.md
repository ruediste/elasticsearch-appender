# Production Grade ElasticSearch Log Appender
This project provides log appenders for various java logging frameworks which directly append to an ElasticSearch (ES) instance. The log messages are buffered in memory and sent to ElasticSearch with bulk requests.

Production Ready means:
* Fixed size buffer. If ElasticSearch slows down, the logs won't fill your memory.
* Monitoring via JMX. You'll see if something goes wrong.
* Tags and Labels. Aggregate logs from multiple servers and still know where your logs are coming from
* Asynchronous Indexing. Your application does not have to wait for logging.
* Time adjustment. Don't loose the order of your logs.

The appender uses a ring buffer based on a fixed size byte array. If it fills up, neither the memory requirements nor the number of objects will go up. This means no increased pressure on garbage collection.

## Setup and Configuration
Add the following Maven dependency to your project:

    <dependency>
      <groupId>com.github.ruediste.elasticsearchAppender</groupId>
      <artifactId>appender</artifactId>
	 <version>???</version>
	</dependency>

### Logback

### Log4j


## Monitoring


    curl -XDELETE http://localhost:9200/logstash-*
    curl -XPOST http://localhost:9200/_template/logstash -d@indexTemplate.json
