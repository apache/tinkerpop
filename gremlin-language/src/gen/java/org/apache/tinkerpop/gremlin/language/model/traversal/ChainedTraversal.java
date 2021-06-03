package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

import org.example.org.apache.tinkerpop.gremlin.language.model.steps.TraversalMethod;

public class ChainedTraversal {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.TraversalMethod
     */
    public final TraversalMethod first;
    
    /**
     * @type list:
     *         union:
     *         - index: 1
     *           name: method
     *           type: org/apache/tinkerpop/gremlin/language/model/steps.TraversalMethod
     *         - index: 2
     *           name: chainedParent
     *           type: org/apache/tinkerpop/gremlin/language/model/traversal.ChainedParentOfGraphTraversal
     */
    public final java.util.List<RestValue> rest;
    
    /**
     * Constructs an immutable ChainedTraversal object
     */
    public ChainedTraversal(TraversalMethod first, java.util.List<RestValue> rest) {
        this.first = first;
        this.rest = rest;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ChainedTraversal)) {
            return false;
        }
        ChainedTraversal o = (ChainedTraversal) other;
        return first.equals(o.first)
            && rest.equals(o.rest);
    }
    
    @Override
    public int hashCode() {
        return 2 * first.hashCode()
            + 3 * rest.hashCode();
    }
    
    /**
     * Construct a new immutable ChainedTraversal object in which first is overridden
     */
    public ChainedTraversal withFirst(TraversalMethod first) {
        return new ChainedTraversal(first, rest);
    }
    
    /**
     * Construct a new immutable ChainedTraversal object in which rest is overridden
     */
    public ChainedTraversal withRest(java.util.List<RestValue> rest) {
        return new ChainedTraversal(first, rest);
    }
    
    public abstract static class RestValue {
        private RestValue() {}
        
        public abstract <R> R accept(Visitor<R> visitor) ;
        
        /**
         * An interface for applying a function to a RestValue according to its variant (subclass)
         */
        public interface Visitor<R> {
            R visit(Method instance) ;
            
            R visit(ChainedParent instance) ;
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
            default R visit(Method instance) {
                return otherwise(instance);
            }
            
            @Override
            default R visit(ChainedParent instance) {
                return otherwise(instance);
            }
        }
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/steps.TraversalMethod
         */
        public static final class Method extends RestValue {
            public final TraversalMethod method;
            
            /**
             * Constructs an immutable Method object
             */
            public Method(TraversalMethod method) {
                this.method = method;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Method)) {
                    return false;
                }
                Method o = (Method) other;
                return method.equals(o.method);
            }
            
            @Override
            public int hashCode() {
                return 2 * method.hashCode();
            }
        }
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.ChainedParentOfGraphTraversal
         */
        public static final class ChainedParent extends RestValue {
            public final ChainedParentOfGraphTraversal chainedParent;
            
            /**
             * Constructs an immutable ChainedParent object
             */
            public ChainedParent(ChainedParentOfGraphTraversal chainedParent) {
                this.chainedParent = chainedParent;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof ChainedParent)) {
                    return false;
                }
                ChainedParent o = (ChainedParent) other;
                return chainedParent.equals(o.chainedParent);
            }
            
            @Override
            public int hashCode() {
                return 2 * chainedParent.hashCode();
            }
        }
    }
}
