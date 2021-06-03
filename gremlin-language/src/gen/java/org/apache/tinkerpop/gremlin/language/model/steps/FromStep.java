package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class FromStep {
    private FromStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a FromStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(FromStepLabel instance) ;
        
        R visit(FromVertex instance) ;
    }
    
    /**
     * An interface for applying a function to a FromStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(FromStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(FromStepLabel instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(FromVertex instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type string
     */
    public static final class FromStepLabel extends FromStep {
        public final String fromStepLabel;
        
        /**
         * Constructs an immutable FromStepLabel object
         */
        public FromStepLabel(String fromStepLabel) {
            this.fromStepLabel = fromStepLabel;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FromStepLabel)) {
                return false;
            }
            FromStepLabel o = (FromStepLabel) other;
            return fromStepLabel.equals(o.fromStepLabel);
        }
        
        @Override
        public int hashCode() {
            return 2 * fromStepLabel.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class FromVertex extends FromStep {
        public final NestedTraversal fromVertex;
        
        /**
         * Constructs an immutable FromVertex object
         */
        public FromVertex(NestedTraversal fromVertex) {
            this.fromVertex = fromVertex;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FromVertex)) {
                return false;
            }
            FromVertex o = (FromVertex) other;
            return fromVertex.equals(o.fromVertex);
        }
        
        @Override
        public int hashCode() {
            return 2 * fromVertex.hashCode();
        }
    }
}
