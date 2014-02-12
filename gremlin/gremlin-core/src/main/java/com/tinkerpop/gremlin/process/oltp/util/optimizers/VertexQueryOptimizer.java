package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.filter.HasStep;
import com.tinkerpop.gremlin.process.oltp.filter.IntervalStep;
import com.tinkerpop.gremlin.process.oltp.filter.RangeStep;
import com.tinkerpop.gremlin.process.oltp.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.oltp.map.IdentityStep;
import com.tinkerpop.gremlin.process.oltp.map.VertexQueryStep;
import com.tinkerpop.gremlin.process.oltp.util.GremlinHelper;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> PIPES_TO_FOLD = new ArrayList<Class>(
            Arrays.asList(
                    IdentityStep.class,
                    HasStep.class,
                    IntervalStep.class,
                    RangeStep.class,
                    EdgeVertexStep.class));

    public boolean optimize(final Traversal traversal, final Step step) {
        if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
            return true;
        else {
            if (GremlinHelper.isLabeled(step))
                return true;
        }

        VertexQueryStep vertexQueryPipe = null;
        for (int i = traversal.getSteps().size() - 1; i >= 0; i--) {
            final Step tempStep = (Step) traversal.getSteps().get(i);
            if (tempStep instanceof VertexQueryStep) {
                vertexQueryPipe = (VertexQueryStep) tempStep;
                break;
            } else if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(tempStep.getClass())).findFirst().isPresent())
                break;
        }

        if (null != vertexQueryPipe && !GremlinHelper.isLabeled(vertexQueryPipe)) {
            if (step instanceof EdgeVertexStep) {
                if (((EdgeVertexStep) step).direction.equals(vertexQueryPipe.queryBuilder.direction.opposite()))
                    vertexQueryPipe.returnClass = Vertex.class;
                else
                    return true;
            } else if (step instanceof HasStep) {
                final HasStep hasPipe = (HasStep) step;
                if (!vertexQueryPipe.returnClass.equals(Vertex.class)) {
                    vertexQueryPipe.queryBuilder.has(hasPipe.hasContainer.key, hasPipe.hasContainer.predicate, hasPipe.hasContainer.value);
                } else
                    return true;
            } else if (step instanceof IntervalStep) {
                final IntervalStep intervalPipe = (IntervalStep) step;
                vertexQueryPipe.queryBuilder.has(intervalPipe.startContainer.key, intervalPipe.startContainer.predicate, intervalPipe.startContainer.value);
                vertexQueryPipe.queryBuilder.has(intervalPipe.endContainer.key, intervalPipe.endContainer.predicate, intervalPipe.endContainer.value);
            } else if (step instanceof RangeStep) {
                final RangeStep rangePipe = (RangeStep) step;
                vertexQueryPipe.low = rangePipe.low;
                vertexQueryPipe.high = rangePipe.high;
            }
            vertexQueryPipe.generateFunction();
            return false;
        }

        return true;
    }
}
