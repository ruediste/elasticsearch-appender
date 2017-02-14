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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
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
     * Amount of time to wait if a small bulk is taken out of the buffer. After
     * the wait, draining is tried again. Avoids always sending the first
     * request in a bulk of size 1.
     * 
     * @see #smallBulkThreshold
     */
    public Duration smallBulkWait = Duration.ofMillis(10);

    /**
     * Number of documents below which a bulk is considered small and the
     * {@link #smallBulkWait} is waited to see if the bulk fills up.
     */
    public int smallBulkThreshold = 50;

    /**
     * Time to wait while stopping until all events are processed
     */
    public Duration stopTimeout = Duration.ofSeconds(10);

    /**
     * URL to connect to ElasticSearch
     */
    public String esUrl = "http://localhost:9200";

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
    private final String name;

    public EsIndexer(String name) {
        this.name = name;
    }

    public void queue(EsIndexRequest request) {
        queue(request.index, request.type, request.payload);
    }

    AtomicLong totalDiscardedCount = new AtomicLong();

    public void queue(String index, String type, String payload) {
        if (softStopping || !started) {
            totalDiscardedCount.incrementAndGet();
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
            totalDiscardedCount.incrementAndGet();
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
                System.err.println(result.getJsonString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
    public long getTotalDiscardedCount() {
        return totalDiscardedCount.get();
    }
}
