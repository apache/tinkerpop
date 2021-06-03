package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.methods.TraversalSackMethod;

/**
 * @type optional:
 *         union:
 *         - index: 1
 *           name: barrierConsumer
 *           type: org/apache/tinkerpop/gremlin/language/model/methods.TraversalSackMethod
 *         - index: 2
 *           name: maxBarrierSize
 *           type: integer
 */
public class BarrierStep {
    public final java.util.Optional<Value> value;
    
    /**
     * Constructs an immutable BarrierStep object
     */
    public BarrierStep(java.util.Optional<Value> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BarrierStep)) {
            return false;
        }
        BarrierStep o = (BarrierStep) other;
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
            R visit(BarrierConsumer instance) ;
            
            R visit(MaxBarrierSize instance) ;
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
            default R visit(BarrierConsumer instance) {
                return otherwise(instance);
            }
            
            @Override
            default R visit(MaxBarrierSize instance) {
                return otherwise(instance);
            }
        }
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/methods.TraversalSackMethod
         */
        public static final class BarrierConsumer extends Value {
            public final TraversalSackMethod barrierConsumer;
            
            /**
             * Constructs an immutable BarrierConsumer object
             */
            public BarrierConsumer(TraversalSackMethod barrierConsumer) {
                this.barrierConsumer = barrierConsumer;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof BarrierConsumer)) {
                    return false;
                }
                BarrierConsumer o = (BarrierConsumer) other;
                return barrierConsumer.equals(o.barrierConsumer);
            }
            
            @Override
            public int hashCode() {
                return 2 * barrierConsumer.hashCode();
            }
        }
        
        /**
         * @type integer
         */
        public static final class MaxBarrierSize extends Value {
            public final Integer maxBarrierSize;
            
            /**
             * Constructs an immutable MaxBarrierSize object
             */
            public MaxBarrierSize(Integer maxBarrierSize) {
                this.maxBarrierSize = maxBarrierSize;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof MaxBarrierSize)) {
                    return false;
                }
                MaxBarrierSize o = (MaxBarrierSize) other;
                return maxBarrierSize.equals(o.maxBarrierSize);
            }
            
            @Override
            public int hashCode() {
                return 2 * maxBarrierSize.hashCode();
            }
        }
    }
}
