package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public class RootTraversal {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalSource
     */
    public final TraversalSource source;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalSourceSpawnMethod
     */
    public final TraversalSourceSpawnMethod spawnMethod;
    
    /**
     * @type optional:
     *         union:
     *         - index: 1
     *           name: chained
     *           type: org/apache/tinkerpop/gremlin/language/model/traversal.ChainedTraversal
     *         - index: 2
     *           name: parent
     *           type: org/apache/tinkerpop/gremlin/language/model/traversal.ChainedParentOfGraphTraversal
     */
    public final java.util.Optional<RestValue> rest;
    
    /**
     * Constructs an immutable RootTraversal object
     */
    public RootTraversal(TraversalSource source, TraversalSourceSpawnMethod spawnMethod, java.util.Optional<RestValue> rest) {
        this.source = source;
        this.spawnMethod = spawnMethod;
        this.rest = rest;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RootTraversal)) {
            return false;
        }
        RootTraversal o = (RootTraversal) other;
        return source.equals(o.source)
            && spawnMethod.equals(o.spawnMethod)
            && rest.equals(o.rest);
    }
    
    @Override
    public int hashCode() {
        return 2 * source.hashCode()
            + 3 * spawnMethod.hashCode()
            + 5 * rest.hashCode();
    }
    
    /**
     * Construct a new immutable RootTraversal object in which source is overridden
     */
    public RootTraversal withSource(TraversalSource source) {
        return new RootTraversal(source, spawnMethod, rest);
    }
    
    /**
     * Construct a new immutable RootTraversal object in which spawnMethod is overridden
     */
    public RootTraversal withSpawnMethod(TraversalSourceSpawnMethod spawnMethod) {
        return new RootTraversal(source, spawnMethod, rest);
    }
    
    /**
     * Construct a new immutable RootTraversal object in which rest is overridden
     */
    public RootTraversal withRest(java.util.Optional<RestValue> rest) {
        return new RootTraversal(source, spawnMethod, rest);
    }
    
    public abstract static class RestValue {
        private RestValue() {}
        
        public abstract <R> R accept(Visitor<R> visitor) ;
        
        /**
         * An interface for applying a function to a RestValue according to its variant (subclass)
         */
        public interface Visitor<R> {
            R visit(Chained instance) ;
            
            R visit(Parent instance) ;
        }
        
        /**
         * An interface for applying a function to a RestValue according to its variant (subclass). If a visit() method for a
         * particular variant is not implemented, a default method is used instead.
         */
        public interface PartialVisitor<R> extends Visitor<R> {
            default R otherwise(RestValue instance) {
                throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
            }
            
            @Override
            default R visit(Chained instance) {
                return otherwise(instance);
            }
            
            @Override
            default R visit(Parent instance) {
                return otherwise(instance);
            }
        }
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.ChainedTraversal
         */
        public static final class Chained extends RestValue {
            public final ChainedTraversal chained;
            
            /**
             * Constructs an immutable Chained object
             */
            public Chained(ChainedTraversal chained) {
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
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.ChainedParentOfGraphTraversal
         */
        public static final class Parent extends RestValue {
            public final ChainedParentOfGraphTraversal parent;
            
            /**
             * Constructs an immutable Parent object
             */
            public Parent(ChainedParentOfGraphTraversal parent) {
                this.parent = parent;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Parent)) {
                    return false;
                }
                Parent o = (Parent) other;
                return parent.equals(o.parent);
            }
            
            @Override
            public int hashCode() {
                return 2 * parent.hashCode();
            }
        }
    }
}
