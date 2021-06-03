package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class ExplainMethod {
    /**
     * Constructs an immutable ExplainMethod object
     */
    public ExplainMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ExplainMethod)) {
            return false;
        }
        ExplainMethod o = (ExplainMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
