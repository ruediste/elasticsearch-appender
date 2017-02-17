# Production Grade ElasticSearch Log Appender
This project provides log appenders for various java logging frameworks which directly append to an ElasticSearch (ES) instance. The log messages are buffered in memory and sent to ElasticSearch with bulk requests.

Production Ready means:
* Fixed size buffer. If ElasticSearch slows down, the logs won't fill your memory.
* Monitoring via JMX. You'll see if something goes wrong.
* Tags and Labels. Aggregate logs from multiple servers and still know where your logs are coming from
* Asynchronous Indexing. Your application does not have to wait for logging.
* Time adjustment. Don't loose the order of your logs.

The appender uses a ring buffer based on a fixed size byte array. If it fills up, neither the memory requirements nor the number of objects will go up. This means no increased pressure on garbage collection.

## Setup
Add the following Maven dependency to your project:

    <dependency>
      <groupId>com.github.ruediste.elasticsearchAppender</groupId>
      <artifactId>appender</artifactId>
  		<version>???</version>
		</dependency>

Then add a configuration using the sample below for your logging system. Each sample configures the appender using the defaults and specifying some tags and labels. One of the tags is taken from an environment variable.

All appenders support the same configuration properties. They are passed to the appenders using the mechanisms of the respective logging system. The individual properties will be discussed afterwards.

### Logback
Sample `logback.xml`:

		<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
		<configuration>
		<appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
		  <encoder>
			  <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n
			  </pattern>
		  </encoder>
		</appender>

		<appender name="ES" class="com.github.ruediste.elasticsearchAppender.logback.EsAppenderLogback">
		  <tags>testTag1,testTag2</tags>
		  <labels>node=testNode,logname=${LOGNAME}</labels>
		</appender>

		<root level="INFO">
		  <appender-ref ref="ES" />
		  <appender-ref ref="STDOUT" />
		</root>

		</configuration>

### Log4j - 1
Sample `log4j.properties`:

		log4j.rootLogger = INFO, ESlog4j, stdout

		log4j.appender.stdout=org.apache.log4j.ConsoleAppender
		log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
		log4j.appender.stdout.layout.ConversionPattern=%r [%t] %-5p %c %x - %m%n

		log4j.appender.ESlog4j=com.github.ruediste.elasticsearchAppender.log4j.EsAppenderLog4j1
		log4j.appender.ESlog4j.Tags=tag1,tag2
		log4j.appender.ESlog4j.Labels=node=foo,logname=${LOGNAME}

Note that environment variable substitution is not supported. The variable is substituted from the system properties.

### Log4j - 2
Sample `log4j2.xml`:

		<?xml version="1.0" encoding="UTF-8"?>
		<Configuration status="INFO">
		    <Appenders>
		        <Console name="Console" target="SYSTEM_OUT">
		            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n" />
		        </Console>

		       <EsAppender name="ESlog4j2" tags="foo,bar" labels="node=n1,logname=${env:LOGNAME}">
		       </EsAppender>
		    </Appenders>
		    <Loggers>
		        <Root level="debug">
		            <AppenderRef ref="Console" />
		            <AppenderRef ref="ESlog4j2" />
		        </Root>
		    </Loggers>
		</Configuration>

## Indexed Documents
The appender sends the logs directly to elasticsearch. The format of the log messages is fixed, but some aspects can be configured. Sample log message:

	{
		"time": 1487311223654,
		"thread": "main",
		"logger": "com.github.ruediste.elasticsearchAppender.log4j.EsAppenderLog4j1Test",
		"message": "Hello World",
		"level": "INFO",
		"mdc": {
		  "test": "testVal"
		},
		"tags": [ "foo", "bar" ],
		"labels": {
		  "node": "n1",
		  "logname": "ruedi"
		}
    }

**Configuration Properties:**
  * **labels:** A comma separated list of strings which is added as tags. Sample: foo,bar
  * **tags:** A comma separated list of key value pairs which is added as labels. Sample: node=n1,stage=prod
  * **esType:** Type of the indexed documents in elasticsearch. Default: log
  * **indexPattern:** Pattern to use for the Index. The pattern is passed to DateTimeFormatter.ofPattern(). Default: 'logstash-'yyyy.MM.dd
  * **maxStringLength:** Strings such as the message or the stack trace are truncated before indexing them. Default value: 10240

## Buffering and Indexing
The documents are serialized and stored in a ring buffer implemented with a fixed byte buffer. A single thread is used to drain documents from the buffer and send them to ElasticSearch using bulk requests. 

If a Request cannot be sent at all, the thread pauses and retries the index operation. If individual documents (even all documents) of a request fail, the failing documents are simply discarded.

If the buffer is full, incoming log messages are rejected (and thus also discarded). No exception is thrown upon rejection. 

**Configuration Properties:**
  * **esUrl:** URL of the ElasticSearch instance to send the log messages to. Default: http://localhost:9200
  * **capacity:** Capacity of the buffer. Values can be specified with a k (kibibytes) of M (mibibytes) suffix. Default: 10M
  * **maxBulkDocumentCount:** Maximum number of documents in a single bulk request. Default: 100
  * **maxBulkMemorySize:** Maximum space the documents of a single bulk request can take up in the ring buffer. This is used to limit the request size of a bulk request. If a single document exceeds this size, it will be indexed with a bulk request with a single document.
  * **stopTimeout:** When an appender is stopped a flag is set indicating the indexing thread to stop. The index loop will continue to index documents as long as there are documents in the buffer. After the stopTimeout, another flag is set indicating the indexing thread to stop immediately. Values are specified as ISO-8601 durations. Default: PT10S
  * **failurePause:** Defines the pause of the indexing thread before retrying a bulk request. Values are specified as ISO-8601 durations. Default: PT10S
  * **name:** name of the appender
  * **threadName:** Name of the thread performing the index operation. If left empty, a name is derived from the appender name

## Monitoring
sliding window...
The operation of the appender can be monitored using JMX. The following values are provided:

  * **TotalEventIndexedCount:**
  * **:**
  * **:**
  * **:**
  * **:**
 

**Configuration Properties:**
  * **:**
  * **:**
  
  
    curl -XDELETE http://localhost:9200/logstash-*
    curl -XPOST http://localhost:9200/_template/logstash -d@indexTemplate.json
