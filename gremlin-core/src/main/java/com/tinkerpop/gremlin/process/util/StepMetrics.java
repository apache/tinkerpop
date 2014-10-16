package com.tinkerpop.gremlin.process.util;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public interface StepMetrics {

    public long getTimeNs();

    public long getTraversers();

    public double getTimeMs();

    public long getCount();

    public String getName();

    public Double getPercentageDuration();
}
