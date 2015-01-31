package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface LoadBalancingStrategy extends Host.Listener {
    public void initialize(final Cluster cluster, final Collection<Host> hosts);

    public Iterator<Host> select(final RequestMessage msg);

    public static class RoundRobin implements LoadBalancingStrategy {

        private final CopyOnWriteArrayList<Host> availableHosts = new CopyOnWriteArrayList<>();
        private final AtomicInteger index = new AtomicInteger();

        @Override
        public void initialize(final Cluster cluster, final Collection<Host> hosts) {
            this.availableHosts.addAll(hosts);
            this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
        }

        @Override
        public Iterator<Host> select(final RequestMessage msg) {
            final List<Host> hosts = (List<Host>) availableHosts.clone();
            final int startIndex = index.getAndIncrement();

            if (startIndex > Integer.MAX_VALUE - 10000)
                index.set(0);

            return new Iterator<Host>() {

                private int currentIndex = startIndex;
                private int remainingHosts = hosts.size();

                @Override
                public boolean hasNext() {
                    return remainingHosts > 0;
                }

                @Override
                public Host next() {
                    remainingHosts--;
                    int c = currentIndex++ % hosts.size();
                    if (c < 0)
                        c += hosts.size();
                    return hosts.get(c);
                }
            };
        }

        @Override
        public void onAvailable(final Host host) {
            this.availableHosts.addIfAbsent(host);
        }

        @Override
        public void onUnavailable(final Host host) {
            this.availableHosts.remove(host);
        }

        @Override
        public void onNew(final Host host) {
            onAvailable(host);
        }

        @Override
        public void onRemove(final Host host) {
            onUnavailable(host);
        }
    }
}
