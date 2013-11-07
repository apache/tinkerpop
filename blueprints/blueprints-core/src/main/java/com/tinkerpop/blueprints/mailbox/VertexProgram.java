package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexProgram<T extends Serializable> {

    public enum KeyType {
        VARIABLE,
        CONSTANT
    }


    public void setup(GraphMemory graphMemory);

    public void execute(Vertex vertex, Mailbox<T> mailbox, GraphMemory graphMemory);

    public boolean terminate(GraphMemory graphMemory);
}
