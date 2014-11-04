package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.giraph.worker.WorkerContext;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphWorkerContext extends WorkerContext {

    private VertexProgram vertexProgram;
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
        this.vertexProgram.workerIterationStart(this.memory);
    }

    public void postSuperstep() {
        this.vertexProgram.workerIterationEnd(this.memory);
    }

    public final VertexProgram getVertexProgram() {
        return this.vertexProgram;
    }

    public final GiraphMemory getMemory() {
        return this.memory;
    }

    public final GiraphMessenger getMessenger(final GiraphComputeVertex giraphComputeVertex, final Iterable<KryoWritable> messages) {
        this.messenger.setCurrentVertex(giraphComputeVertex, messages);
        return this.messenger;
    }
}
