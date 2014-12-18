package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public interface TraversalMetrics {
    public static final String METRICS_KEY = Graph.System.system("metrics");

    public long getDuration(TimeUnit unit);
    public Metrics getMetrics(final int stepIndex);
    public Metrics getMetrics(final String stepLabel);
}
