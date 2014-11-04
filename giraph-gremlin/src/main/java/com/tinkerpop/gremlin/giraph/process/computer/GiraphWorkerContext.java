package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.giraph.worker.WorkerContext;

import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphWorkerContext extends WorkerContext {

    private VertexProgram vertexProgram;
    private GiraphMemory memory;
    private long workerId;

    public GiraphWorkerContext() {
        // Giraph ReflectionUtils requires this to be public at minimum
    }

    public void preApplication() throws InstantiationException, IllegalAccessException {
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getContext().getConfiguration()));
        this.memory = new GiraphMemory(this, this.vertexProgram);
        this.workerId = UUID.randomUUID().getLeastSignificantBits();
    }

    public void postApplication() {
        // do nothing
    }

    public void preSuperstep() {
        this.vertexProgram.workerStartup(this.memory);
    }

    public void postSuperstep() {
        this.vertexProgram.workerStartup(this.memory);
    }

    public long getWorkerId() {
        return this.workerId;
    }
}
