package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

/**
 * @type optional:
 *         union:
 *         - index: 1
 *           name: emitPredicate
 *           type: org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
 *         - index: 2
 *           name: emitTraversal
 *           type: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
 */
public class EmitStep {
    public final java.util.Optional<Value> value;
    
    /**
     * Constructs an immutable EmitStep object
     */
    public EmitStep(java.util.Optional<Value> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EmitStep)) {
            return false;
        }
        EmitStep o = (EmitStep) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
    
    public abstract static class Value {
        private Value() {}
        
        public abstract <R> R accept(Visitor<R> visitor) ;
        
        /**
         * An interface for applying a function to a Value according to its variant (subclass)
         */
        public interface Visitor<R> {
            R visit(EmitPredicate instance) ;
            
            R visit(EmitTraversal instance) ;
        }
        
        /**
         * An interface for applying a function to a Value according to its variant (subclass). If a visit() method for a
         * particular variant is not implemented, a default method is used instead.
         */
        public interface PartialVisitor<R> extends Visitor<R> {
            default R otherwise(Value instance) {
                throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
            }
            
            @Override
            default R visit(EmitPredicate instance) {
                return otherwise(instance);
            }
            
            @Override
            default R visit(EmitTraversal instance) {
                return otherwise(instance);
            }
        }
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/predicates.TraversalPredicate
         */
        public static final class EmitPredicate extends Value {
            public final TraversalPredicate emitPredicate;
            
            /**
             * Constructs an immutable EmitPredicate object
             */
            public EmitPredicate(TraversalPredicate emitPredicate) {
                this.emitPredicate = emitPredicate;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof EmitPredicate)) {
                    return false;
                }
                EmitPredicate o = (EmitPredicate) other;
                return emitPredicate.equals(o.emitPredicate);
            }
            
            @Override
            public int hashCode() {
                return 2 * emitPredicate.hashCode();
            }
        }
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
         */
        public static final class EmitTraversal extends Value {
            public final NestedTraversal emitTraversal;
            
            /**
             * Constructs an immutable EmitTraversal object
             */
            public EmitTraversal(NestedTraversal emitTraversal) {
                this.emitTraversal = emitTraversal;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof EmitTraversal)) {
                    return false;
                }
                EmitTraversal o = (EmitTraversal) other;
                return emitTraversal.equals(o.emitTraversal);
            }
            
            @Override
            public int hashCode() {
                return 2 * emitTraversal.hashCode();
            }
        }
    }
}
