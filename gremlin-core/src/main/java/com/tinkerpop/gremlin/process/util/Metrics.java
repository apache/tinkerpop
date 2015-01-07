package com.tinkerpop.gremlin.process.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Holds metrics data; typically for .profile()-step analysis. Metrics may be nested. Nesting enables the ability to
 * capture explicit metrics for multiple distinct operations. Annotations are used to store miscellaneous notes that
 * might be useful to a developer when examining results, such as index coverage for Steps in a Traversal.
 *
 * @author Bob Briody (http://bobbriody.com)
 */
public interface Metrics {

    /**
     * Get the duration of execution time taken.
     *
     * @param units
     * @return
     */
    public long getDuration(TimeUnit units);

    /**
     * The number of items operated upon. For a Travseral Step, this will be the number of Traversers (which may be less
     * than the number of Elements when Elements are bulked).
     *
     * @return Number of items operated upon.
     */
    public long getCount();

    /**
     * Name of this Metrics.
     *
     * @return name of this Metrics.
     */
    public String getName();

    /**
     * If this Metrics object has multiple peer Metrics then this value will represent the percentage of the total
     * duration taken by this Metrics object.
     *
     * @return
     */
    public double getPercentDuration();

    /**
     * Get the nested Metrics objects.
     *
     * @return the nested Metrics objects.
     */
    public Collection<MetricsUtil> getChildren();

    /**
     * Get a nested Metrics object by Id.
     *
     * @param metricsId
     * @return a nested Metrics object.
     */
    MetricsUtil getChild(String metricsId);

    /**
     * Obtain the annotations for this Metrics.
     *
     * @return the annotations for this Metrics. Modifications to the returned object are persisted in the original.
     */
    public Map<String, String> getAnnotations();
}
