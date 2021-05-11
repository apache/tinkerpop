package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalComparator;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalFunction;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalOrder;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalToken;

/**
 * @type optional:
 *         union:
 *         - index: 1
 *           name: withTraversal
 *           type:
 *             record:
 *             - index: 1
 *               name: traversal
 *               type: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
 *             - index: 2
 *               name: comparator
 *               type: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalComparator
 *         - index: 2
 *           name: withKey
 *           type:
 *             record:
 *             - index: 1
 *               name: key
 *               type: string
 *             - index: 2
 *               name: comparator
 *               type:
 *                 optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalComparator
 *         - index: 3
 *           name: withFunction
 *           type:
 *             record:
 *             - index: 1
 *               name: function
 *               type: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalFunction
 *             - index: 2
 *               name: comparator
 *               type:
 *                 optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalComparator
 *         - index: 4
 *           name: token
 *           type: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalToken
 *         - index: 5
 *           name: comparator
 *           type: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalComparator
 *         - index: 6
 *           name: order
 *           type: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalOrder
 */
public class ByStep {
    public final java.util.Optional<Value> value;
    
    /**
     * Constructs an immutable ByStep object
     */
    public ByStep(java.util.Optional<Value> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByStep)) return false;
        ByStep o = (ByStep) other;
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
            R visit(WithTraversal instance) ;
            
            R visit(WithKey instance) ;
            
            R visit(WithFunction instance) ;
            
            R visit(Token instance) ;
            
            R visit(Comparator instance) ;
            
            R visit(Order instance) ;
        }
        
        /**
         * An interface for applying a function to a Value according to its variant (subclass). If a visit() method for a
         * particular variant is not implemented, a default method is used instead.
         */
        public interface PartialVisitor<R> extends Visitor<R> {
            default R otherwise(Value instance) {
                throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
            }
            
            default R visit(WithTraversal instance) {
                return otherwise(instance);
            }
            
            default R visit(WithKey instance) {
                return otherwise(instance);
            }
            
            default R visit(WithFunction instance) {
                return otherwise(instance);
            }
            
            default R visit(Token instance) {
                return otherwise(instance);
            }
            
            default R visit(Comparator instance) {
                return otherwise(instance);
            }
            
            default R visit(Order instance) {
                return otherwise(instance);
            }
        }
        
        public static final class WithTraversal extends Value {
            public final WithTraversalValue withTraversal;
            
            /**
             * Constructs an immutable WithTraversal object
             */
            public WithTraversal(WithTraversalValue withTraversal) {
                this.withTraversal = withTraversal;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof WithTraversal)) return false;
                WithTraversal o = (WithTraversal) other;
                return withTraversal.equals(o.withTraversal);
            }
            
            @Override
            public int hashCode() {
                return 2 * withTraversal.hashCode();
            }
        }
        
        public static final class WithKey extends Value {
            public final WithKeyValue withKey;
            
            /**
             * Constructs an immutable WithKey object
             */
            public WithKey(WithKeyValue withKey) {
                this.withKey = withKey;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof WithKey)) return false;
                WithKey o = (WithKey) other;
                return withKey.equals(o.withKey);
            }
            
            @Override
            public int hashCode() {
                return 2 * withKey.hashCode();
            }
        }
        
        public static final class WithFunction extends Value {
            public final WithFunctionValue withFunction;
            
            /**
             * Constructs an immutable WithFunction object
             */
            public WithFunction(WithFunctionValue withFunction) {
                this.withFunction = withFunction;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof WithFunction)) return false;
                WithFunction o = (WithFunction) other;
                return withFunction.equals(o.withFunction);
            }
            
            @Override
            public int hashCode() {
                return 2 * withFunction.hashCode();
            }
        }
        
        public static final class Token extends Value {
            public final TraversalToken token;
            
            /**
             * Constructs an immutable Token object
             */
            public Token(TraversalToken token) {
                this.token = token;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Token)) return false;
                Token o = (Token) other;
                return token.equals(o.token);
            }
            
            @Override
            public int hashCode() {
                return 2 * token.hashCode();
            }
        }
        
        public static final class Comparator extends Value {
            public final TraversalComparator comparator;
            
            /**
             * Constructs an immutable Comparator object
             */
            public Comparator(TraversalComparator comparator) {
                this.comparator = comparator;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Comparator)) return false;
                Comparator o = (Comparator) other;
                return comparator.equals(o.comparator);
            }
            
            @Override
            public int hashCode() {
                return 2 * comparator.hashCode();
            }
        }
        
        public static final class Order extends Value {
            public final TraversalOrder order;
            
            /**
             * Constructs an immutable Order object
             */
            public Order(TraversalOrder order) {
                this.order = order;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Order)) return false;
                Order o = (Order) other;
                return order.equals(o.order);
            }
            
            @Override
            public int hashCode() {
                return 2 * order.hashCode();
            }
        }
    }
    
    public static class WithFunctionValue {
        public final TraversalFunction function;
        
        public final java.util.Optional<TraversalComparator> comparator;
        
        /**
         * Constructs an immutable WithFunctionValue object
         */
        public WithFunctionValue(TraversalFunction function, java.util.Optional<TraversalComparator> comparator) {
            this.function = function;
            this.comparator = comparator;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithFunctionValue)) return false;
            WithFunctionValue o = (WithFunctionValue) other;
            return function.equals(o.function)
                && comparator.equals(o.comparator);
        }
        
        @Override
        public int hashCode() {
            return 2 * function.hashCode()
                + 3 * comparator.hashCode();
        }
        
        /**
         * Construct a new immutable WithFunctionValue object in which function is overridden
         */
        public WithFunctionValue withFunction(TraversalFunction function) {
            return new WithFunctionValue(function, comparator);
        }
        
        /**
         * Construct a new immutable WithFunctionValue object in which comparator is overridden
         */
        public WithFunctionValue withComparator(java.util.Optional<TraversalComparator> comparator) {
            return new WithFunctionValue(function, comparator);
        }
    }
    
    public static class WithKeyValue {
        public final String key;
        
        public final java.util.Optional<TraversalComparator> comparator;
        
        /**
         * Constructs an immutable WithKeyValue object
         */
        public WithKeyValue(String key, java.util.Optional<TraversalComparator> comparator) {
            this.key = key;
            this.comparator = comparator;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithKeyValue)) return false;
            WithKeyValue o = (WithKeyValue) other;
            return key.equals(o.key)
                && comparator.equals(o.comparator);
        }
        
        @Override
        public int hashCode() {
            return 2 * key.hashCode()
                + 3 * comparator.hashCode();
        }
        
        /**
         * Construct a new immutable WithKeyValue object in which key is overridden
         */
        public WithKeyValue withKey(String key) {
            return new WithKeyValue(key, comparator);
        }
        
        /**
         * Construct a new immutable WithKeyValue object in which comparator is overridden
         */
        public WithKeyValue withComparator(java.util.Optional<TraversalComparator> comparator) {
            return new WithKeyValue(key, comparator);
        }
    }
    
    public static class WithTraversalValue {
        public final NestedTraversal traversal;
        
        public final TraversalComparator comparator;
        
        /**
         * Constructs an immutable WithTraversalValue object
         */
        public WithTraversalValue(NestedTraversal traversal, TraversalComparator comparator) {
            this.traversal = traversal;
            this.comparator = comparator;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithTraversalValue)) return false;
            WithTraversalValue o = (WithTraversalValue) other;
            return traversal.equals(o.traversal)
                && comparator.equals(o.comparator);
        }
        
        @Override
        public int hashCode() {
            return 2 * traversal.hashCode()
                + 3 * comparator.hashCode();
        }
        
        /**
         * Construct a new immutable WithTraversalValue object in which traversal is overridden
         */
        public WithTraversalValue withTraversal(NestedTraversal traversal) {
            return new WithTraversalValue(traversal, comparator);
        }
        
        /**
         * Construct a new immutable WithTraversalValue object in which comparator is overridden
         */
        public WithTraversalValue withComparator(TraversalComparator comparator) {
            return new WithTraversalValue(traversal, comparator);
        }
    }
}
