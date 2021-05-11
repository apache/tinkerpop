package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.predicates.TraversalPredicate;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalToken;

public abstract class HasStep {
    private HasStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithLabelValue {
        public final String label;
        
        public final String propertyKey;
        
        public final WithLabelOtherValue other;
        
        /**
         * Constructs an immutable WithLabelValue object
         */
        public WithLabelValue(String label, String propertyKey, WithLabelOtherValue other) {
            this.label = label;
            this.propertyKey = propertyKey;
            this.other = other;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithLabelValue)) return false;
            WithLabelValue o = (WithLabelValue) other;
            return label.equals(o.label)
                && propertyKey.equals(o.propertyKey)
                && other.equals(o.other);
        }
        
        @Override
        public int hashCode() {
            return 2 * label.hashCode()
                + 3 * propertyKey.hashCode()
                + 5 * other.hashCode();
        }
        
        /**
         * Construct a new immutable WithLabelValue object in which label is overridden
         */
        public WithLabelValue withLabel(String label) {
            return new WithLabelValue(label, propertyKey, other);
        }
        
        /**
         * Construct a new immutable WithLabelValue object in which propertyKey is overridden
         */
        public WithLabelValue withPropertyKey(String propertyKey) {
            return new WithLabelValue(label, propertyKey, other);
        }
        
        /**
         * Construct a new immutable WithLabelValue object in which other is overridden
         */
        public WithLabelValue withOther(WithLabelOtherValue other) {
            return new WithLabelValue(label, propertyKey, other);
        }
    }
    
    public abstract static class WithLabelOtherValue {
        private WithLabelOtherValue() {}
        
        public abstract <R> R accept(Visitor<R> visitor) ;
        
        /**
         * An interface for applying a function to a WithLabelOtherValue according to its variant (subclass)
         */
        public interface Visitor<R> {
            R visit(Predicate instance) ;
            
            R visit(Value instance) ;
        }
        
        /**
         * An interface for applying a function to a WithLabelOtherValue according to its variant (subclass). If a visit()
         * method for a particular variant is not implemented, a default method is used instead.
         */
        public interface PartialVisitor<R> extends Visitor<R> {
            default R otherwise(WithLabelOtherValue instance) {
                throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
            }
            
            default R visit(Predicate instance) {
                return otherwise(instance);
            }
            
            default R visit(Value instance) {
                return otherwise(instance);
            }
        }
        
        public static final class Predicate extends WithLabelOtherValue {
            public final TraversalPredicate predicate;
            
            /**
             * Constructs an immutable Predicate object
             */
            public Predicate(TraversalPredicate predicate) {
                this.predicate = predicate;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Predicate)) return false;
                Predicate o = (Predicate) other;
                return predicate.equals(o.predicate);
            }
            
            @Override
            public int hashCode() {
                return 2 * predicate.hashCode();
            }
        }
        
        public static final class Value extends WithLabelOtherValue {
            public final GenericLiteral value;
            
            /**
             * Constructs an immutable Value object
             */
            public Value(GenericLiteral value) {
                this.value = value;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Value)) return false;
                Value o = (Value) other;
                return value.equals(o.value);
            }
            
            @Override
            public int hashCode() {
                return 2 * value.hashCode();
            }
        }
    }
    
    public static class WithAccessorValue {
        public final TraversalToken accessor;
        
        public final WithAccessorOtherValue other;
        
        /**
         * Constructs an immutable WithAccessorValue object
         */
        public WithAccessorValue(TraversalToken accessor, WithAccessorOtherValue other) {
            this.accessor = accessor;
            this.other = other;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithAccessorValue)) return false;
            WithAccessorValue o = (WithAccessorValue) other;
            return accessor.equals(o.accessor)
                && other.equals(o.other);
        }
        
        @Override
        public int hashCode() {
            return 2 * accessor.hashCode()
                + 3 * other.hashCode();
        }
        
        /**
         * Construct a new immutable WithAccessorValue object in which accessor is overridden
         */
        public WithAccessorValue withAccessor(TraversalToken accessor) {
            return new WithAccessorValue(accessor, other);
        }
        
        /**
         * Construct a new immutable WithAccessorValue object in which other is overridden
         */
        public WithAccessorValue withOther(WithAccessorOtherValue other) {
            return new WithAccessorValue(accessor, other);
        }
    }
    
    public abstract static class WithAccessorOtherValue {
        private WithAccessorOtherValue() {}
        
        public abstract <R> R accept(Visitor<R> visitor) ;
        
        /**
         * An interface for applying a function to a WithAccessorOtherValue according to its variant (subclass)
         */
        public interface Visitor<R> {
            R visit(Predicate instance) ;
            
            R visit(Value instance) ;
            
            R visit(Traversal instance) ;
        }
        
        /**
         * An interface for applying a function to a WithAccessorOtherValue according to its variant (subclass). If a visit()
         * method for a particular variant is not implemented, a default method is used instead.
         */
        public interface PartialVisitor<R> extends Visitor<R> {
            default R otherwise(WithAccessorOtherValue instance) {
                throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
            }
            
            default R visit(Predicate instance) {
                return otherwise(instance);
            }
            
            default R visit(Value instance) {
                return otherwise(instance);
            }
            
            default R visit(Traversal instance) {
                return otherwise(instance);
            }
        }
        
        public static final class Predicate extends WithAccessorOtherValue {
            public final TraversalPredicate predicate;
            
            /**
             * Constructs an immutable Predicate object
             */
            public Predicate(TraversalPredicate predicate) {
                this.predicate = predicate;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Predicate)) return false;
                Predicate o = (Predicate) other;
                return predicate.equals(o.predicate);
            }
            
            @Override
            public int hashCode() {
                return 2 * predicate.hashCode();
            }
        }
        
        public static final class Value extends WithAccessorOtherValue {
            public final GenericLiteral value;
            
            /**
             * Constructs an immutable Value object
             */
            public Value(GenericLiteral value) {
                this.value = value;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Value)) return false;
                Value o = (Value) other;
                return value.equals(o.value);
            }
            
            @Override
            public int hashCode() {
                return 2 * value.hashCode();
            }
        }
        
        public static final class Traversal extends WithAccessorOtherValue {
            public final NestedTraversal traversal;
            
            /**
             * Constructs an immutable Traversal object
             */
            public Traversal(NestedTraversal traversal) {
                this.traversal = traversal;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Traversal)) return false;
                Traversal o = (Traversal) other;
                return traversal.equals(o.traversal);
            }
            
            @Override
            public int hashCode() {
                return 2 * traversal.hashCode();
            }
        }
    }
    
    public static class WithPropertyKeyValue {
        public final String propertyKey;
        
        public final java.util.Optional<WithPropertyKeyOtherArgumentsValue> otherArguments;
        
        /**
         * Constructs an immutable WithPropertyKeyValue object
         */
        public WithPropertyKeyValue(String propertyKey, java.util.Optional<WithPropertyKeyOtherArgumentsValue> otherArguments) {
            this.propertyKey = propertyKey;
            this.otherArguments = otherArguments;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithPropertyKeyValue)) return false;
            WithPropertyKeyValue o = (WithPropertyKeyValue) other;
            return propertyKey.equals(o.propertyKey)
                && otherArguments.equals(o.otherArguments);
        }
        
        @Override
        public int hashCode() {
            return 2 * propertyKey.hashCode()
                + 3 * otherArguments.hashCode();
        }
        
        /**
         * Construct a new immutable WithPropertyKeyValue object in which propertyKey is overridden
         */
        public WithPropertyKeyValue withPropertyKey(String propertyKey) {
            return new WithPropertyKeyValue(propertyKey, otherArguments);
        }
        
        /**
         * Construct a new immutable WithPropertyKeyValue object in which otherArguments is overridden
         */
        public WithPropertyKeyValue withOtherArguments(java.util.Optional<WithPropertyKeyOtherArgumentsValue> otherArguments) {
            return new WithPropertyKeyValue(propertyKey, otherArguments);
        }
    }
    
    public abstract static class WithPropertyKeyOtherArgumentsValue {
        private WithPropertyKeyOtherArgumentsValue() {}
        
        public abstract <R> R accept(Visitor<R> visitor) ;
        
        /**
         * An interface for applying a function to a WithPropertyKeyOtherArgumentsValue according to its variant (subclass)
         */
        public interface Visitor<R> {
            R visit(Predicate instance) ;
            
            R visit(Value instance) ;
            
            R visit(Traversal instance) ;
        }
        
        /**
         * An interface for applying a function to a WithPropertyKeyOtherArgumentsValue according to its variant (subclass). If
         * a visit() method for a particular variant is not implemented, a default method is used instead.
         */
        public interface PartialVisitor<R> extends Visitor<R> {
            default R otherwise(WithPropertyKeyOtherArgumentsValue instance) {
                throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
            }
            
            default R visit(Predicate instance) {
                return otherwise(instance);
            }
            
            default R visit(Value instance) {
                return otherwise(instance);
            }
            
            default R visit(Traversal instance) {
                return otherwise(instance);
            }
        }
        
        public static final class Predicate extends WithPropertyKeyOtherArgumentsValue {
            public final TraversalPredicate predicate;
            
            /**
             * Constructs an immutable Predicate object
             */
            public Predicate(TraversalPredicate predicate) {
                this.predicate = predicate;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Predicate)) return false;
                Predicate o = (Predicate) other;
                return predicate.equals(o.predicate);
            }
            
            @Override
            public int hashCode() {
                return 2 * predicate.hashCode();
            }
        }
        
        public static final class Value extends WithPropertyKeyOtherArgumentsValue {
            public final GenericLiteral value;
            
            /**
             * Constructs an immutable Value object
             */
            public Value(GenericLiteral value) {
                this.value = value;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Value)) return false;
                Value o = (Value) other;
                return value.equals(o.value);
            }
            
            @Override
            public int hashCode() {
                return 2 * value.hashCode();
            }
        }
        
        public static final class Traversal extends WithPropertyKeyOtherArgumentsValue {
            public final NestedTraversal traversal;
            
            /**
             * Constructs an immutable Traversal object
             */
            public Traversal(NestedTraversal traversal) {
                this.traversal = traversal;
            }
            
            @Override
            public <R> R accept(Visitor<R> visitor) {
                return visitor.visit(this);
            }
            
            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Traversal)) return false;
                Traversal o = (Traversal) other;
                return traversal.equals(o.traversal);
            }
            
            @Override
            public int hashCode() {
                return 2 * traversal.hashCode();
            }
        }
    }
    
    /**
     * An interface for applying a function to a HasStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(WithPropertyKey instance) ;
        
        R visit(WithAccessor instance) ;
        
        R visit(WithLabel instance) ;
    }
    
    /**
     * An interface for applying a function to a HasStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(HasStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(WithPropertyKey instance) {
            return otherwise(instance);
        }
        
        default R visit(WithAccessor instance) {
            return otherwise(instance);
        }
        
        default R visit(WithLabel instance) {
            return otherwise(instance);
        }
    }
    
    public static final class WithPropertyKey extends HasStep {
        public final WithPropertyKeyValue withPropertyKey;
        
        /**
         * Constructs an immutable WithPropertyKey object
         */
        public WithPropertyKey(WithPropertyKeyValue withPropertyKey) {
            this.withPropertyKey = withPropertyKey;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithPropertyKey)) return false;
            WithPropertyKey o = (WithPropertyKey) other;
            return withPropertyKey.equals(o.withPropertyKey);
        }
        
        @Override
        public int hashCode() {
            return 2 * withPropertyKey.hashCode();
        }
    }
    
    public static final class WithAccessor extends HasStep {
        public final WithAccessorValue withAccessor;
        
        /**
         * Constructs an immutable WithAccessor object
         */
        public WithAccessor(WithAccessorValue withAccessor) {
            this.withAccessor = withAccessor;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithAccessor)) return false;
            WithAccessor o = (WithAccessor) other;
            return withAccessor.equals(o.withAccessor);
        }
        
        @Override
        public int hashCode() {
            return 2 * withAccessor.hashCode();
        }
    }
    
    public static final class WithLabel extends HasStep {
        public final WithLabelValue withLabel;
        
        /**
         * Constructs an immutable WithLabel object
         */
        public WithLabel(WithLabelValue withLabel) {
            this.withLabel = withLabel;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithLabel)) return false;
            WithLabel o = (WithLabel) other;
            return withLabel.equals(o.withLabel);
        }
        
        @Override
        public int hashCode() {
            return 2 * withLabel.hashCode();
        }
    }
}
