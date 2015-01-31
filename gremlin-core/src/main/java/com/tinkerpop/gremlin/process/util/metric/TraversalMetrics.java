package com.tinkerpop.gremlin.process.util.metric;

import com.tinkerpop.gremlin.process.util.metric.Metrics;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Contains the Metrics gathered for a Traversal as the result of the .profile()-step.
 *
 * @author Bob Briody (http://bobbriody.com)
 */
public interface TraversalMetrics {
    /**
     * The side-effect key used to store and retrieve the TraversalMetrics for a given Traversal.
     */
    public static final String METRICS_KEY = Graph.Hidden.hide("metrics");


    /**
     * The MetricsId used to obtain the element count via Metrics.getNested(String metricsId)
     */
    public static final String ELEMENT_COUNT_ID = "elementCount";

    /**
     * Get the total duration taken by the Traversal.
     *
     * @param unit
     * @return total duration taken by the Traversal.
     */
    public long getDuration(TimeUnit unit);

    /**
     * Get an individual Metrics object by the index of the profiled Step.
     *
     * @param stepIndex
     * @return an individual Metrics object.
     */
    public Metrics getMetrics(final int stepIndex);

    /**
     * Get an individual Metrics object by the label of the profiled Step.
     *
     * @param stepLabel
     * @return an individual Metrics object.
     */
    public Metrics getMetrics(final String stepLabel);

    public Collection<? extends Metrics> getMetrics();
}
