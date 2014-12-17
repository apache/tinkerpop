package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public interface TraversalMetrics {
    public static final String METRICS_KEY = Graph.System.system("metrics");

    public double getDurationMs();
    public StepMetrics getStepMetrics(final int stepIndex);
    public StepMetrics getStepMetrics(final String stepLabel);
}
