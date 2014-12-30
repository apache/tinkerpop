package com.tinkerpop.gremlin.process.util;

import java.util.Collection;
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

    public String getAnnotation(String key);
}
