/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Singleton that contains and configures Gremlin Server's {@code MetricRegistry}. Borrowed from Titan's approach to
 * managing Metrics.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum MetricManager {
    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(MetricManager.class);

    private final MetricRegistry registry = new MetricRegistry();
    private ConsoleReporter consoleReporter = null;
    private CsvReporter csvReporter = null;
    private JmxReporter jmxReporter = null;
    private Slf4jReporter slf4jReporter = null;
    private GangliaReporter gangliaReporter = null;
    private GraphiteReporter graphiteReporter = null;

    /**
     * Return the {@code MetricsRegistry}.
     *
     * @return the single {@code MetricRegistry} used for all of monitoring
     */
    public MetricRegistry getRegistry() {
        return registry;
    }

    /**
     * Create a {@link ConsoleReporter} attached to the {@code MetricsRegistry}.
     *
     * @param reportIntervalInMS milliseconds to wait between dumping metrics to the console
     */
    public synchronized void addConsoleReporter(final long reportIntervalInMS) {
        if (null != consoleReporter) {
            log.debug("Metrics ConsoleReporter already active; not creating another");
            return;
        }

        consoleReporter = ConsoleReporter.forRegistry(getRegistry()).build();
        consoleReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);

        log.info("Configured Metrics ConsoleReporter configured with report interval={}ms", reportIntervalInMS);
    }

    /**
     * Stop a {@link ConsoleReporter} previously created by a call to
     * {@link #addConsoleReporter(long)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeConsoleReporter() {
        if (null != consoleReporter)
            consoleReporter.stop();

        consoleReporter = null;
    }

    /**
     * Create a {@link CsvReporter} attached to the {@code MetricsRegistry}.
     * <p/>
     * The {@code output} argument must be non-null but need not exist. If it
     * doesn't already exist, this method attempts to create it by calling
     * {@link File#mkdirs()}.
     *
     * @param reportIntervalInMS milliseconds to wait between dumping metrics to CSV files in
     *                           the configured directory
     * @param output             the path to a directory into which Metrics will periodically
     *                           write CSV data
     */
    public synchronized void addCsvReporter(final long reportIntervalInMS,
                                            final String output) {

        File outputDir = new File(output);

        if (null != csvReporter) {
            log.debug("Metrics CsvReporter already active; not creating another");
            return;
        }

        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                log.warn("Failed to create CSV metrics dir {}", outputDir);
            }
        }

        csvReporter = CsvReporter.forRegistry(getRegistry()).build(outputDir);
        csvReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);

        log.info("Configured Metrics CsvReporter configured with report interval={}ms to fileName={}", reportIntervalInMS, output);
    }

    /**
     * Stop a {@link CsvReporter} previously created by a call to
     * {@link #addCsvReporter(long, String)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeCsvReporter() {
        if (null != csvReporter)
            csvReporter.stop();

        csvReporter = null;
    }

    /**
     * Create a {@link JmxReporter} attached to the {@code MetricsRegistry}.
     * <p/>
     * If {@code domain} or {@code agentId} is null, then Metrics's uses its own
     * internal default value(s).
     * <p/>
     * If {@code agentId} is non-null, then
     * MBeanServerFactory#findMBeanServer(agentId) must return exactly
     * one {@code MBeanServer}. The reporter will register with that server. If
     * the {@code findMBeanServer(agentId)} call returns no or multiple servers,
     * then this method logs an error and falls back on the Metrics default for
     * {@code agentId}.
     *
     * @param domain  the JMX domain in which to continuously expose metrics
     * @param agentId the JMX agent ID
     */
    public synchronized void addJmxReporter(final String domain, final String agentId) {
        if (null != jmxReporter) {
            log.debug("Metrics JmxReporter already active; not creating another");
            return;
        }

        JmxReporter.Builder b = JmxReporter.forRegistry(getRegistry());

        if (null != domain) {
            b.inDomain(domain);
        }

        if (null != agentId) {
            List<MBeanServer> servs = MBeanServerFactory.findMBeanServer(agentId);
            if (null != servs && 1 == servs.size()) {
                b.registerWith(servs.get(0));
            } else {
                log.error("Metrics Slf4jReporter agentId {} does not resolve to a single MBeanServer", agentId);
            }
        }

        jmxReporter = b.build();
        jmxReporter.start();

        log.info("Configured Metrics JmxReporter configured with domain={} and agentId={}",
                Optional.ofNullable(domain).orElse(""), Optional.ofNullable(agentId).orElse(""));
    }

    /**
     * Stop a {@link JmxReporter} previously created by a call to
     * {@link #addJmxReporter(String, String)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeJmxReporter() {
        if (null != jmxReporter)
            jmxReporter.stop();

        jmxReporter = null;
    }

    /**
     * Create a {@link Slf4jReporter} attached to the {@code MetricsRegistry}.
     * <p/>
     * If {@code loggerName} is null, or if it is non-null but
     * LoggerFactory.getLogger(loggerName) returns null, then Metrics's
     * default Slf4j logger name is used instead.
     *
     * @param reportIntervalInMS milliseconds to wait between writing metrics to the Slf4j
     *                           logger
     * @param loggerName         the name of the Slf4j logger that receives metrics
     */
    public synchronized void addSlf4jReporter(final long reportIntervalInMS, final String loggerName) {
        if (null != slf4jReporter) {
            log.debug("Metrics Slf4jReporter already active; not creating another");
            return;
        }

        Slf4jReporter.Builder b = Slf4jReporter.forRegistry(getRegistry());

        if (null != loggerName) {
            Logger l = LoggerFactory.getLogger(loggerName);
            if (null != l) {
                b.outputTo(l);
            } else {
                log.error("Logger with name {} could not be obtained", loggerName);
            }
        }

        slf4jReporter = b.build();
        slf4jReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);

        log.info("Configured Metrics Slf4jReporter configured with interval={}ms and loggerName={}", reportIntervalInMS, loggerName);
    }

    /**
     * Stop a {@link Slf4jReporter} previously created by a call to
     * {@link #addSlf4jReporter(long, String)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeSlf4jReporter() {
        if (null != slf4jReporter)
            slf4jReporter.stop();

        slf4jReporter = null;
    }

    /**
     * Create a {@link GangliaReporter} attached to the {@code MetricsRegistry}.
     * <p/>
     * {@code groupOrHost} and {@code addressingMode} must be non-null. The
     * remaining non-primitive arguments may be null. If {@code protocol31} is
     * null, then true is assumed. Null values of {@code hostUUID} or
     * {@code spoof} are passed into the {@link GMetric} constructor, which
     * causes Ganglia to use its internal logic for generating a default UUID
     * and default reporting hostname (respectively).
     *
     * @param groupOrHost        the multicast group or unicast hostname to which Ganglia
     *                           events are sent
     * @param port               the port to which events are sent
     * @param addressingMode     whether to send events with multicast or unicast
     * @param ttl                multicast ttl (ignored for unicast)
     * @param protocol31         true to use Ganglia protocol version 3.1, false to use 3.0
     * @param hostUUID           uuid for the host
     * @param spoof              override this machine's IP/hostname as it appears on the
     *                           Ganglia server
     * @param reportIntervalInMS milliseconds to wait before sending data to the ganglia
     *                           unicast host or multicast group
     * @throws IOException when a {@link GMetric} can't be instantiated using the
     *                     provided arguments
     */
    public synchronized void addGangliaReporter(final String groupOrHost, final int port,
                                                final String addressingMode, final int ttl, final Boolean protocol31,
                                                final UUID hostUUID, final String spoof, final long reportIntervalInMS) throws IOException {
        if (null == groupOrHost || groupOrHost.isEmpty())
            throw new IllegalArgumentException("groupOrHost cannot be null or empty");

        if (null == addressingMode)
            throw new IllegalArgumentException("addressing mode cannot be null");

        GMetric.UDPAddressingMode gmetricAddressingMode;
        try {
            gmetricAddressingMode = GMetric.UDPAddressingMode.valueOf(addressingMode);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("addressing mode must be MULTICAST or UNICAST");
        }

        if (null != gangliaReporter) {
            log.debug("Metrics GangliaReporter already active; not creating another");
            return;
        }

        final boolean protocol = null == protocol31 ? true : protocol31;
        final GMetric ganglia = new GMetric(groupOrHost, port, gmetricAddressingMode, ttl,
                protocol, hostUUID, spoof);

        final GangliaReporter.Builder b = GangliaReporter.forRegistry(getRegistry());

        gangliaReporter = b.build(ganglia);
        gangliaReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);

        log.info("Configured Ganglia Metrics reporter host={} interval={}ms port={} addrmode={} ttl={} proto31={} uuid={} spoof={}",
                new Object[]{groupOrHost, reportIntervalInMS, port, addressingMode, ttl, protocol31, hostUUID, spoof});
    }

    /**
     * Stop a {@link GangliaReporter} previously created by a call to
     * addGangliaReporter(String, int, GMetric.UDPAddressingMode, int, Boolean, UUID, long)
     * and release it for GC. Idempotent between calls to the associated add
     * method. Does nothing before the first call to the associated add method.
     */
    public synchronized void removeGangliaReporter() {
        if (null != gangliaReporter)
            gangliaReporter.stop();

        gangliaReporter = null;
    }

    /**
     * Create a {@link GraphiteReporter} attached to the {@code MetricsRegistry}.
     * <p/>
     * If {@code prefix} is null, then Metrics's internal default prefix is used
     * (empty string at the time this comment was written).
     *
     * @param host               the host to which Graphite reports are sent
     * @param port               the port to which Graphite reports are sent
     * @param prefix             the optional metrics prefix
     * @param reportIntervalInMS milliseconds to wait between sending metrics to the configured
     *                           Graphite host and port
     */
    public synchronized void addGraphiteReporter(final String host, final int port,
                                                 final String prefix, final long reportIntervalInMS) {
        if (host == null || host.isEmpty())
            throw new IllegalArgumentException("Host cannot be null or empty");

        Graphite graphite = new Graphite(new InetSocketAddress(host, port));

        GraphiteReporter.Builder b = GraphiteReporter
                .forRegistry(getRegistry());

        if (null != prefix)
            b.prefixedWith(prefix);

        b.filter(MetricFilter.ALL);

        graphiteReporter = b.build(graphite);
        graphiteReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);
        log.info("Configured Graphite reporter host={} interval={}ms port={} prefix={}",
                new Object[]{host, reportIntervalInMS, port, prefix});
    }

    /**
     * Stop a {@link GraphiteReporter} previously created by a call to
     * {@link #addGraphiteReporter(String, int, String, long)} and release it
     * for GC. Idempotent between calls to the associated add method. Does
     * nothing before the first call to the associated add method.
     */
    public synchronized void removeGraphiteReporter() {
        if (null != graphiteReporter)
            graphiteReporter.stop();

        graphiteReporter = null;
    }

    /**
     * Remove all reporters previously configured through the {@code add*} methods on this class.
     */
    public synchronized void removeAllReporters() {
        removeConsoleReporter();
        removeCsvReporter();
        removeJmxReporter();
        removeSlf4jReporter();
        removeGangliaReporter();
        removeGraphiteReporter();
    }

    public synchronized void removeAllMetrics() {
        getRegistry().removeMatching((s, metric) -> true);
    }

    public Counter getCounter(final String name) {
        return getRegistry().counter(name);
    }

    public Counter getCounter(final String prefix, final String... names) {
        return getRegistry().counter(MetricRegistry.name(prefix, names));
    }

    public <T> Gauge<T> getGuage(final Gauge<T> gauge, final String name) {
        return getRegistry().register(name, gauge);
    }

    public <T> Gauge<T> getGuage(final Gauge<T> gauge, final String prefix, final String... names) {
        return getRegistry().register(MetricRegistry.name(prefix, names), gauge);
    }

    public Meter getMeter(final String name) {
        return getRegistry().meter(name);
    }

    public Meter getMeter(final String prefix, final String... names) {
        return getRegistry().meter(MetricRegistry.name(prefix, names));
    }

    public Timer getTimer(final String name) {
        return getRegistry().timer(name);
    }

    public Timer getTimer(final String prefix, final String... names) {
        return getRegistry().timer(MetricRegistry.name(prefix, names));
    }

    public Histogram getHistogram(final String name) {
        return getRegistry().histogram(name);
    }

    public Histogram getHistogram(final String prefix, final String... names) {
        return getRegistry().histogram(MetricRegistry.name(prefix, names));
    }

    /**
     * Registers metrics from a {@link GremlinScriptEngine}. At this point, this only works for the
     * {@link GremlinGroovyScriptEngine} as it is the only one that collects metrics at this point. As the
     * {@link GremlinScriptEngine} implementations achieve greater parity these metrics will get expanded.
     */
    public void registerGremlinScriptEngineMetrics(final GremlinScriptEngine engine, final String... prefix) {
        // only register if metrics aren't already registered. typically only happens in testing where two gremlin
        // server instances are running in the same jvm. they will share the same metrics if that is the case since
        // the MetricsManager is static
        if (engine instanceof GremlinGroovyScriptEngine && getRegistry().getNames().stream().noneMatch(n -> n.endsWith("long-run-compilation-count"))) {
            final GremlinGroovyScriptEngine gremlinGroovyScriptEngine = (GremlinGroovyScriptEngine) engine;
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "long-run-compilation-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLongRunCompilationCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "estimated-size")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheEstimatedSize);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "average-load-penalty")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheAverageLoadPenalty);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "eviction-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheEvictionCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "eviction-weight")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheEvictionWeight);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "hit-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheHitCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "hit-rate")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheHitRate);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLoadCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-failure-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLoadFailureCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-failure-rate")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheLoadFailureRate);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-success-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLoadSuccessCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "miss-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheMissCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "miss-rate")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheMissRate);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "request-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheRequestCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "total-load-time")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheTotalLoadTime);
        }
    }
}
