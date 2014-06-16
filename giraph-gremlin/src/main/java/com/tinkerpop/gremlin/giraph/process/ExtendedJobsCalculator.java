package com.tinkerpop.gremlin.giraph.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ExtendedJobsCalculator {

    public List<Job> determineExtendedJobs(final Configuration configuration);
}
