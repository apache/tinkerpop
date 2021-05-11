package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public abstract class AddVStep {
    private AddVStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class NullValue {
        /**
         * Constructs an immutable NullValue object
         */
        public NullValue() {}
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NullValue)) return false;
            NullValue o = (NullValue) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
    
    public static class EmptyValue {
        /**
         * Constructs an immutable EmptyValue object
         */
        public EmptyValue() {}
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EmptyValue)) return false;
            EmptyValue o = (EmptyValue) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
    
    /**
     * An interface for applying a function to a AddVStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Empty instance) ;
        
        R visit(VertexLabel instance) ;
        
        R visit(Null instance) ;
        
        R visit(VertexLabelTraversal instance) ;
    }
    
    /**
     * An interface for applying a function to a AddVStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(AddVStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Empty instance) {
            return otherwise(instance);
        }
        
        default R visit(VertexLabel instance) {
            return otherwise(instance);
        }
        
        default R visit(Null instance) {
            return otherwise(instance);
        }
        
        default R visit(VertexLabelTraversal instance) {
            return otherwise(instance);
        }
    }
    
    public static final class Empty extends AddVStep {
        public final EmptyValue empty;
        
        /**
         * Constructs an immutable Empty object
         */
        public Empty(EmptyValue empty) {
            this.empty = empty;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Empty)) return false;
            Empty o = (Empty) other;
            return empty.equals(o.empty);
        }
        
        @Override
        public int hashCode() {
            return 2 * empty.hashCode();
        }
    }
    
    /**
     * @type string
     */
    public static final class VertexLabel extends AddVStep {
        public final String vertexLabel;
        
        /**
         * Constructs an immutable VertexLabel object
         */
        public VertexLabel(String vertexLabel) {
            this.vertexLabel = vertexLabel;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof VertexLabel)) return false;
            VertexLabel o = (VertexLabel) other;
            return vertexLabel.equals(o.vertexLabel);
        }
        
        @Override
        public int hashCode() {
            return 2 * vertexLabel.hashCode();
        }
    }
    
    public static final class Null extends AddVStep {
        public final NullValue nullEsc;
        
        /**
         * Constructs an immutable Null object
         */
        public Null(NullValue nullEsc) {
            this.nullEsc = nullEsc;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Null)) return false;
            Null o = (Null) other;
            return nullEsc.equals(o.nullEsc);
        }
        
        @Override
        public int hashCode() {
            return 2 * nullEsc.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class VertexLabelTraversal extends AddVStep {
        public final NestedTraversal vertexLabelTraversal;
        
        /**
         * Constructs an immutable VertexLabelTraversal object
         */
        public VertexLabelTraversal(NestedTraversal vertexLabelTraversal) {
            this.vertexLabelTraversal = vertexLabelTraversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof VertexLabelTraversal)) return false;
            VertexLabelTraversal o = (VertexLabelTraversal) other;
            return vertexLabelTraversal.equals(o.vertexLabelTraversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * vertexLabelTraversal.hashCode();
        }
    }
}
