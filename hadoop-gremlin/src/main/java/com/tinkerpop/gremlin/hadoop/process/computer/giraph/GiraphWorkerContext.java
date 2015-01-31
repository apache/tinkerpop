package com.tinkerpop.gremlin.hadoop.process.computer.giraph;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.ImmutableMemory;
import org.apache.giraph.worker.WorkerContext;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphWorkerContext extends WorkerContext {

    private VertexProgram<?> vertexProgram;
    private GiraphMemory memory;
    private GiraphMessenger messenger;

    public GiraphWorkerContext() {
        // Giraph ReflectionUtils requires this to be public at minimum
    }

    public void preApplication() throws InstantiationException, IllegalAccessException {
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getContext().getConfiguration()));
        this.memory = new GiraphMemory(this, this.vertexProgram);
        this.messenger = new GiraphMessenger();
    }

    public void postApplication() {

    }

    public void preSuperstep() {
        this.vertexProgram.workerIterationStart(new ImmutableMemory(this.memory));
    }

    public void postSuperstep() {
        this.vertexProgram.workerIterationEnd(new ImmutableMemory(this.memory));
    }

    public VertexProgram<?> getVertexProgram() {
        return this.vertexProgram;
    }

    public GiraphMemory getMemory() {
        return this.memory;
    }

    public GiraphMessenger getMessenger(final GiraphComputeVertex giraphComputeVertex, final Iterable<ObjectWritable> messages) {
        this.messenger.setCurrentVertex(giraphComputeVertex, messages);
        return this.messenger;
    }
}
