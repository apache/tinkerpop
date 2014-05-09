package com.tinkerpop.gremlin.groovy.console;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.Item;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Mediator {

    private Cluster cluster = null;

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
            // todo: timeout
            return client.submit(gremlin).all().get();
        } catch (final Exception ex) {
             throw ex;
        } finally {
            // todo: close client?
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
