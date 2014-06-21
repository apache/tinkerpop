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
public class TraversalExtraJobsCalculator implements ExtraJobsCalculator {

    public TraversalExtraJobsCalculator() {

    }

    public List<Job> deriveExtraJobs(final Configuration configuration) {
        final List<Job> jobs = new ArrayList<>();
        final VertexProgram program = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(configuration));
        if (program instanceof TraversalVertexProgram) {
            final Traversal traversal = (Traversal) ((TraversalVertexProgram) program).getTraversalSupplier().get();
            traversal.strategies().applyFinalOptimizers(traversal);
            traversal.getSteps().forEach(step -> {
                if (step instanceof JobCreator) {
                    try {
                        jobs.add(((JobCreator) step).createJob(configuration));
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            });
            // TODO: Dah.. GraphSON.
            /*try {
                jobs.add(new TraversalResultMapReduce().createJob(configuration));
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }*/
        } else throw new IllegalStateException("The provided vertex program is not a TraversalVertexProgram");
        return jobs;
    }
}
