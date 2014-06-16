package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalExtendedJobsCalculator implements ExtendedJobsCalculator {

    public TraversalExtendedJobsCalculator() {

    }

    public List<Job> determineExtendedJobs(final Configuration configuration) {
        final List<Job> jobs = new ArrayList<>();
        final VertexProgram program = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(configuration));
        if (program instanceof TraversalVertexProgram) {
            Traversal traversal = (Traversal) ((TraversalVertexProgram) program).getTraversalSupplier().get();
            traversal.strategies().applyFinalOptimizers(traversal);
            traversal.getSteps().forEach(step -> {
                if (step instanceof ExtendsJob) {
                    try {
                        final Job job = new Job(configuration, step + ":SideEffect");
                        ((ExtendsJob) step).configureJob(job, configuration);
                        jobs.add(job);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            });
        }
        return jobs;
    }
}
