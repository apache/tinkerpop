package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class AddEStep {
    private AddEStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a AddEStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(EdgeLabel instance) ;
        
        R visit(EdgeLabelTraversal instance) ;
    }
    
    /**
     * An interface for applying a function to a AddEStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(AddEStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(EdgeLabel instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(EdgeLabelTraversal instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type string
     */
    public static final class EdgeLabel extends AddEStep {
        public final String edgeLabel;
        
        /**
         * Constructs an immutable EdgeLabel object
         */
        public EdgeLabel(String edgeLabel) {
            this.edgeLabel = edgeLabel;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EdgeLabel)) {
                return false;
            }
            EdgeLabel o = (EdgeLabel) other;
            return edgeLabel.equals(o.edgeLabel);
        }
        
        @Override
        public int hashCode() {
            return 2 * edgeLabel.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class EdgeLabelTraversal extends AddEStep {
        public final NestedTraversal edgeLabelTraversal;
        
        /**
         * Constructs an immutable EdgeLabelTraversal object
         */
        public EdgeLabelTraversal(NestedTraversal edgeLabelTraversal) {
            this.edgeLabelTraversal = edgeLabelTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EdgeLabelTraversal)) {
                return false;
            }
            EdgeLabelTraversal o = (EdgeLabelTraversal) other;
            return edgeLabelTraversal.equals(o.edgeLabelTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * edgeLabelTraversal.hashCode();
        }
    }
}
