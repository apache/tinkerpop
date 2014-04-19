package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A queue of incoming {@link ResponseMessage} objects.  The queue is updated by the
 * {@link Handler.GremlinResponseDecoder} until a response terminator is identified.  At that point the fetch
 * status is changed to {@link Status#COMPLETE} and all results have made it client side.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ResponseQueue {
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
                msg = responseQueue.poll(10, TimeUnit.MILLISECONDS);
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
