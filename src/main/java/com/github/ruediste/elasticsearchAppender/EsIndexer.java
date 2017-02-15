package com.github.ruediste.elasticsearchAppender;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Throwables;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.core.Index;

/**
 * Read from a {@link EsIndexRequestRingBuffer}
 * 
 * <img src="doc-files/overview.png" alt="">
 */
public class EsIndexer implements EsIndexerMBean {

    private static final Charset utf8 = Charset.forName("UTF-8");
    private static final AtomicInteger nextThreadNr = new AtomicInteger();

    EsIndexRequestRingBuffer buffer;

    /**
     * Name of the thread to perform the index requests. If null, a thread name
     * will be generated.
     */
    public String threadName;

    /**
     * Capacity of the buffer. Defaults to 10MiB
     */
    public int capacity = 10 * 1024 * 1024;

    /**
     * Number of documents to maximally include in a bulk. Defaults to 100
     * 
     * @see #smallBulkThreshold
     */
    public int maxBulkDocumentCount = 100;

    /**
     * Maximum memory size the requests in a bulk can take up in the buffer.
     * Avoids sending bulks with few large documents. Initialized to 1MiB
     */
    public int maxBulkMemorySize = 1 * 1024 * 1024;

    /**
     * Time to wait while stopping until all events are processed
     */
    public Duration stopTimeout = Duration.ofSeconds(10);

    /**
     * URL to connect to ElasticSearch
     */
    public String esUrl = "http://localhost:9200";

    public int maxStringLength;
    /**
     * If true (default), an JMX MBean will be registered to monitor this
     * indexer
     */
    public boolean performJMXRegistration = true;
    /**
     * Name to use for the mBean registration. If null, a name will be generated
     */
    public String mBeanName;

    /**
     * Client to connect to ElasticSearch. If set to null, a new client will be
     * created during {@link #start()} using {@link #esUrl}
     */
    public JestClient jestClient;

    /**
     * Name of this indexer. Used to derive other names from
     */
    public String name;

    /**
     * Logger callback to enable logging by the indexer
     */
    public EsIndexerLogger logger;

    public int getMaxStringLength() {
        return maxStringLength;
    }

    public void setMaxStringLength(int maxStringLength) {
        this.maxStringLength = maxStringLength;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getMaxBulkDocumentCount() {
        return maxBulkDocumentCount;
    }

    public void setMaxBulkDocumentCount(int maxBulkDocumentCount) {
        this.maxBulkDocumentCount = maxBulkDocumentCount;
    }

    public int getMaxBulkMemorySize() {
        return maxBulkMemorySize;
    }

    public void setMaxBulkMemorySize(int maxBulkMemorySize) {
        this.maxBulkMemorySize = maxBulkMemorySize;
    }

    public Duration getStopTimeout() {
        return stopTimeout;
    }

    public void setStopTimeout(Duration stopTimeout) {
        this.stopTimeout = stopTimeout;
    }

    public String getEsUrl() {
        return esUrl;
    }

    public void setEsUrl(String esUrl) {
        this.esUrl = esUrl;
    }

    public boolean isPerformJMXRegistration() {
        return performJMXRegistration;
    }

    public void setPerformJMXRegistration(boolean performJMXRegistration) {
        this.performJMXRegistration = performJMXRegistration;
    }

    public String getmBeanName() {
        return mBeanName;
    }

    public void setmBeanName(String mBeanName) {
        this.mBeanName = mBeanName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EsIndexer() {

    }

    public EsIndexer(String name, EsIndexerLogger logger) {
        this.name = name;
        this.logger = logger;
    }

    public void queue(EsIndexRequest request) {
        queue(request.index, request.type, request.payload);
    }

    private AtomicLong totalEventDiscardedCount = new AtomicLong();

    public void queue(String index, String type, String payload) {
        if (softStopping || !started) {
            totalEventDiscardedCount.incrementAndGet();
            return;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(index);
            dos.writeUTF(type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!buffer.put(baos.toByteArray(), payload.getBytes(utf8))) {
            // discarded
            totalEventDiscardedCount.incrementAndGet();
        }
    }

    public synchronized void start() {
        if (started)
            return;
        softStopping = false;
        hardStopping = false;
        stopped = new CountDownLatch(1);
        started = true;
        createBuffer();
        if (performJMXRegistration) {
            try {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                registeredMBeanName = new ObjectName(mBeanName != null ? mBeanName
                        : "com.github.ruediste.elasticsearchAppender:type=esIndexer,name=" + this.name);
                mbs.registerMBean(this, registeredMBeanName);

            } catch (Throwable t) {
                throw new RuntimeException("Error while registering MBean", t);
            }

        }
        if (jestClient == null) {
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder(esUrl).multiThreaded(true).build());
            jestClient = factory.getObject();
        }
        Thread thread = new Thread(this::indexingLoop,
                threadName != null ? threadName : "esLogIndexer-" + name + "-" + nextThreadNr.getAndIncrement());
        thread.setDaemon(true);
        thread.start();
    }

    void createBuffer() {
        buffer = new EsIndexRequestRingBuffer(capacity);
    }

    private volatile boolean started;
    private volatile boolean softStopping;
    private volatile boolean hardStopping;
    private CountDownLatch stopped;
    private ObjectName registeredMBeanName;

    public synchronized void stop() {
        if (!started)
            return;
        softStopping = true;
        try {
            if (!stopped.await(stopTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                hardStopping = true;
                stopped.await(4, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            started = false;
        }
        if (registeredMBeanName != null) {
            try {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                mbs.unregisterMBean(registeredMBeanName);
                registeredMBeanName = null;
            } catch (Throwable t) {
                throw new RuntimeException("Error while unregistering MBean", t);
            }
        }
    }

    private boolean indexingFailing = false;
    private volatile long totalEventIndexingFailedCount;
    private volatile long totalEventIndexedCount;

    private void indexingLoop() {
        try {
            while (!hardStopping) {
                List<byte[]> elements = buffer.drain(maxBulkDocumentCount, maxBulkMemorySize, Duration.ofSeconds(1));
                if (hardStopping || elements.isEmpty() && softStopping)
                    return;
                processElements(elements);
            }
        } finally {
            stopped.countDown();
        }

    }

    protected void processElements(List<byte[]> elements) {
        if (elements.size() == 0)
            return;
        Bulk.Builder bulk = new Bulk.Builder();
        for (byte[] element : elements) {
            EsIndexRequest req = toIndexRequest(element);
            bulk.addAction(new Index.Builder(req.payload).index(req.index).type(req.type).build());
        }
        try {
            BulkResult result = jestClient.execute(bulk.build());
            if (!result.isSucceeded()) {
                List<BulkResultItem> failedItems = result.getFailedItems();
                totalEventIndexingFailedCount += failedItems.size();
                totalEventIndexedCount += (elements.size() - failedItems.size());
                if (!indexingFailing) {
                    String message = "Errors in bulk index request. Bulk contained " + elements.size() + " documents, "
                            + failedItems.size() + " failed. Bulk error message: " + result.getErrorMessage() + ".";
                    if (failedItems.size() > 0) {
                        message += " Error message of first failed item: " + failedItems.get(0).error;
                    }
                    logger.error(message + " Continuing to try, but suppressing log output");
                    indexingFailing = true;
                }
            } else {
                totalEventIndexedCount += elements.size();
                if (indexingFailing) {
                    logger.info("Indexing successful for the first time after a failure");
                    indexingFailing = false;
                }
            }
        } catch (IOException e) {
            totalEventIndexingFailedCount += elements.size();
            if (!indexingFailing) {
                String trace = Throwables.getStackTraceAsString(e);
                logger.error("Error while connecting to elastic search. Continuing to try but suppressing output.\n"
                        + trace);
                indexingFailing = true;
            }
        }
    }

    EsIndexRequest toIndexRequest(byte[] element) {
        EsIndexRequest request = new EsIndexRequest();
        ByteArrayInputStream bais = new ByteArrayInputStream(element);
        try (DataInputStream dis = new DataInputStream(bais)) {
            request.index = dis.readUTF();
            request.type = dis.readUTF();
            byte[] tmp = new byte[bais.available()];
            bais.read(tmp);
            request.payload = new String(tmp, utf8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return request;
    }

    @Override
    public long getTotalEventDiscardedCount() {
        return totalEventDiscardedCount.get();
    }

    @Override
    public long getTotalEventIndexingFailedCount() {
        return totalEventIndexingFailedCount;
    }

    @Override
    public long getQueueLength() {
        return buffer.availableElements();
    }

    @Override
    public long getTotalEventIndexedCount() {
        return totalEventIndexedCount;
    }

    @Override
    public double getQueueFillFraction() {
        return buffer.usedCapacityFraction();
    }

    @Override
    public void resetStatistics() {
        totalEventDiscardedCount.set(0);
        totalEventIndexedCount = 0;
        totalEventIndexingFailedCount = 0;
    }

    /**
     * Truncate a string to {@link #getMaxStringLength()}
     */
    public String truncate(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() > maxStringLength) {
            return s.substring(0, maxStringLength) + "...";
        }
        return s;
    }

    private static ThreadLocal<Long> lastLogTimeStamp = new ThreadLocal<>();

    /**
     * adjust time stamp such that only one event is logged for the same milli
     * second
     */
    public void calcNextTimestamp(long origTimeStamp, LongConsumer timestampConsumer, LongConsumer adjustmentConsumer) {
        long timeStamp = origTimeStamp;
        {
            Long lastStamp = lastLogTimeStamp.get();
            if (lastStamp != null && lastStamp >= timeStamp) {
                timeStamp = lastStamp + 1;
                adjustmentConsumer.accept(timeStamp - origTimeStamp);
            }
            lastLogTimeStamp.set(timeStamp);

            timestampConsumer.accept(timeStamp);
        }

    }
}
