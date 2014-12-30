package com.tinkerpop.gremlin.process.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public interface Metrics {

    public long getDuration(TimeUnit units);

    public long getCount();

    public String getName();

    public double getPercentDuration();

    public Collection<MetricsUtil> getChildren();

    MetricsUtil getChild(String metricsId);

    /**
     * Obtain the annotations for this Metrics.
     *
     * @return the annotations for this Metrics. Modifications to the returned object are persisted in the original.
     */
    public Map<String, String> getAnnotations();
}
