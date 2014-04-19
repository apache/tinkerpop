package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseQueue {
    public enum Status {
        FETCHING,
        COMPLETE
    }

    private final LinkedBlockingQueue<ResponseMessage> responseQueue;

    private volatile Status status = Status.FETCHING;

    public ResponseQueue(final LinkedBlockingQueue<ResponseMessage> responseQueue) {
        this.responseQueue = responseQueue;
    }

    public void add(final ResponseMessage msg) {
        this.responseQueue.offer(msg);
    }

    public int size() {
        return this.responseQueue.size();
    }

    public ResponseMessage poll() {
        try {
            ResponseMessage msg;
            do {
                msg = responseQueue.poll(100, TimeUnit.MILLISECONDS);
            } while (null == msg && status == Status.FETCHING);

            return msg;
        } catch (InterruptedException ie) {
            throw new RuntimeException("Taking too long to get results");
        }
    }

    public Status getStatus() {
        return status;
    }

    void markComplete() {
        this.status = Status.COMPLETE;
    }
}
