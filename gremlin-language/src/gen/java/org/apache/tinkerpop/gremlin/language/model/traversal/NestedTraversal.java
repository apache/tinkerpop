package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class NestedTraversal {
    private NestedTraversal() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class ChainedValue {
        /**
         * @type boolean
         */
        public final Boolean anonymous;
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.ChainedTraversal
         */
        public final ChainedTraversal traversal;
        
        /**
         * Constructs an immutable ChainedValue object
         */
        public ChainedValue(Boolean anonymous, ChainedTraversal traversal) {
            this.anonymous = anonymous;
            this.traversal = traversal;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ChainedValue)) {
                return false;
            }
            ChainedValue o = (ChainedValue) other;
            return anonymous.equals(o.anonymous)
                && traversal.equals(o.traversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * anonymous.hashCode()
                + 3 * traversal.hashCode();
        }
        
        /**
         * Construct a new immutable ChainedValue object in which anonymous is overridden
         */
        public ChainedValue withAnonymous(Boolean anonymous) {
            return new ChainedValue(anonymous, traversal);
        }
        
        /**
         * Construct a new immutable ChainedValue object in which traversal is overridden
         */
        public ChainedValue withTraversal(ChainedTraversal traversal) {
            return new ChainedValue(anonymous, traversal);
        }
    }
    
    /**
     * An interface for applying a function to a NestedTraversal according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Root instance) ;
        
        R visit(Chained instance) ;
    }
    
    /**
     * An interface for applying a function to a NestedTraversal according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(NestedTraversal instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Root instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Chained instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.RootTraversal
     */
    public static final class Root extends NestedTraversal {
        public final RootTraversal root;
        
        /**
         * Constructs an immutable Root object
         */
        public Root(RootTraversal root) {
            this.root = root;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Root)) {
                return false;
            }
            Root o = (Root) other;
            return root.equals(o.root);
        }
        
        @Override
        public int hashCode() {
            return 2 * root.hashCode();
        }
    }
    
    public static final class Chained extends NestedTraversal {
        public final ChainedValue chained;
        
        /**
         * Constructs an immutable Chained object
         */
        public Chained(ChainedValue chained) {
            this.chained = chained;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Chained)) {
                return false;
            }
            Chained o = (Chained) other;
            return chained.equals(o.chained);
        }
        
        @Override
        public int hashCode() {
            return 2 * chained.hashCode();
        }
    }
}
