package com.tinkerpop.gremlin.groovy.console;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.Item;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Mediator {

    private Cluster cluster = null;
    public int remoteTimeout = 180000;

    public void clusterSelected(final Cluster cluster) {
        this.close();
        this.cluster = cluster;
        this.cluster.init();
    }

    public boolean isClusterConfigured() {
        return cluster != null;
    }

    public List<Item> submit(final String gremlin) throws Exception {
        final Client client = cluster.connect();
        try {
            return client.submit(gremlin).all().get(remoteTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException toe) {
            throw new RuntimeException("request timed out while processing - increase the timeout with the :remote command");
        } finally {
            try {
                client.close();
            } catch (Exception ex) {
                // empty
            }
        }
    }

    public String clusterInfo() {
        return this.cluster.toString();
    }

    public CompletableFuture<Void> close() {
        if (this.cluster != null)
            return this.cluster.closeAsync();

        return CompletableFuture.completedFuture(null);
    }


}
