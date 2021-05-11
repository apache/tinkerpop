package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class TraversalStrategy {
    private TraversalStrategy() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class ReadOnlyValue {
        /**
         * Constructs an immutable ReadOnlyValue object
         */
        public ReadOnlyValue() {}
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ReadOnlyValue)) return false;
            ReadOnlyValue o = (ReadOnlyValue) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
    
    /**
     * An interface for applying a function to a TraversalStrategy according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Partition instance) ;
        
        R visit(Subgraph instance) ;
        
        R visit(EdgeLabelVerification instance) ;
        
        R visit(ReadOnly instance) ;
        
        R visit(ReservedKeysVerification instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalStrategy according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalStrategy instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Partition instance) {
            return otherwise(instance);
        }
        
        default R visit(Subgraph instance) {
            return otherwise(instance);
        }
        
        default R visit(EdgeLabelVerification instance) {
            return otherwise(instance);
        }
        
        default R visit(ReadOnly instance) {
            return otherwise(instance);
        }
        
        default R visit(ReservedKeysVerification instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.PartitionStrategy
     */
    public static final class Partition extends TraversalStrategy {
        public final java.util.List<PartitionStrategy> partition;
        
        /**
         * Constructs an immutable Partition object
         */
        public Partition(java.util.List<PartitionStrategy> partition) {
            this.partition = partition;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Partition)) return false;
            Partition o = (Partition) other;
            return partition.equals(o.partition);
        }
        
        @Override
        public int hashCode() {
            return 2 * partition.hashCode();
        }
    }
    
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.SubgraphStrategy
     */
    public static final class Subgraph extends TraversalStrategy {
        public final java.util.List<SubgraphStrategy> subgraph;
        
        /**
         * Constructs an immutable Subgraph object
         */
        public Subgraph(java.util.List<SubgraphStrategy> subgraph) {
            this.subgraph = subgraph;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Subgraph)) return false;
            Subgraph o = (Subgraph) other;
            return subgraph.equals(o.subgraph);
        }
        
        @Override
        public int hashCode() {
            return 2 * subgraph.hashCode();
        }
    }
    
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.EdgeLabelVerificationStrategy
     */
    public static final class EdgeLabelVerification extends TraversalStrategy {
        public final java.util.List<EdgeLabelVerificationStrategy> edgeLabelVerification;
        
        /**
         * Constructs an immutable EdgeLabelVerification object
         */
        public EdgeLabelVerification(java.util.List<EdgeLabelVerificationStrategy> edgeLabelVerification) {
            this.edgeLabelVerification = edgeLabelVerification;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EdgeLabelVerification)) return false;
            EdgeLabelVerification o = (EdgeLabelVerification) other;
            return edgeLabelVerification.equals(o.edgeLabelVerification);
        }
        
        @Override
        public int hashCode() {
            return 2 * edgeLabelVerification.hashCode();
        }
    }
    
    public static final class ReadOnly extends TraversalStrategy {
        public final ReadOnlyValue readOnly;
        
        /**
         * Constructs an immutable ReadOnly object
         */
        public ReadOnly(ReadOnlyValue readOnly) {
            this.readOnly = readOnly;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ReadOnly)) return false;
            ReadOnly o = (ReadOnly) other;
            return readOnly.equals(o.readOnly);
        }
        
        @Override
        public int hashCode() {
            return 2 * readOnly.hashCode();
        }
    }
    
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.ReservedKeysVerificationStrategy
     */
    public static final class ReservedKeysVerification extends TraversalStrategy {
        public final java.util.List<ReservedKeysVerificationStrategy> reservedKeysVerification;
        
        /**
         * Constructs an immutable ReservedKeysVerification object
         */
        public ReservedKeysVerification(java.util.List<ReservedKeysVerificationStrategy> reservedKeysVerification) {
            this.reservedKeysVerification = reservedKeysVerification;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ReservedKeysVerification)) return false;
            ReservedKeysVerification o = (ReservedKeysVerification) other;
            return reservedKeysVerification.equals(o.reservedKeysVerification);
        }
        
        @Override
        public int hashCode() {
            return 2 * reservedKeysVerification.hashCode();
        }
    }
}
