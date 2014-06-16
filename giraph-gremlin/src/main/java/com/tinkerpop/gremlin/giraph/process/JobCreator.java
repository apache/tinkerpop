package com.tinkerpop.gremlin.giraph.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface JobCreator {

    public Job createJob(final Configuration configuration) throws IOException;
}
