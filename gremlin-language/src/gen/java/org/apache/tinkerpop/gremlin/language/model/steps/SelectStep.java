package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalColumn;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalPop;

public abstract class SelectStep {
    private SelectStep() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class TraversalValue {
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalPop
         */
        public final java.util.Optional<TraversalPop> pop;
        
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
         */
        public final NestedTraversal traversal;
        
        /**
         * Constructs an immutable TraversalValue object
         */
        public TraversalValue(java.util.Optional<TraversalPop> pop, NestedTraversal traversal) {
            this.pop = pop;
            this.traversal = traversal;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TraversalValue)) {
                return false;
            }
            TraversalValue o = (TraversalValue) other;
            return pop.equals(o.pop)
                && traversal.equals(o.traversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * pop.hashCode()
                + 3 * traversal.hashCode();
        }
        
        /**
         * Construct a new immutable TraversalValue object in which pop is overridden
         */
        public TraversalValue withPop(java.util.Optional<TraversalPop> pop) {
            return new TraversalValue(pop, traversal);
        }
        
        /**
         * Construct a new immutable TraversalValue object in which traversal is overridden
         */
        public TraversalValue withTraversal(NestedTraversal traversal) {
            return new TraversalValue(pop, traversal);
        }
    }
    
    public static class KeysValue {
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalPop
         */
        public final java.util.Optional<TraversalPop> pop;
        
        /**
         * @type string
         */
        public final String selectKey1;
        
        /**
         * @type string
         */
        public final String selectKey2;
        
        /**
         * @type list: string
         */
        public final java.util.List<String> otherSelectKeys;
        
        /**
         * Constructs an immutable KeysValue object
         */
        public KeysValue(java.util.Optional<TraversalPop> pop, String selectKey1, String selectKey2, java.util.List<String> otherSelectKeys) {
            this.pop = pop;
            this.selectKey1 = selectKey1;
            this.selectKey2 = selectKey2;
            this.otherSelectKeys = otherSelectKeys;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof KeysValue)) {
                return false;
            }
            KeysValue o = (KeysValue) other;
            return pop.equals(o.pop)
                && selectKey1.equals(o.selectKey1)
                && selectKey2.equals(o.selectKey2)
                && otherSelectKeys.equals(o.otherSelectKeys);
        }
        
        @Override
        public int hashCode() {
            return 2 * pop.hashCode()
                + 3 * selectKey1.hashCode()
                + 5 * selectKey2.hashCode()
                + 7 * otherSelectKeys.hashCode();
        }
        
        /**
         * Construct a new immutable KeysValue object in which pop is overridden
         */
        public KeysValue withPop(java.util.Optional<TraversalPop> pop) {
            return new KeysValue(pop, selectKey1, selectKey2, otherSelectKeys);
        }
        
        /**
         * Construct a new immutable KeysValue object in which selectKey1 is overridden
         */
        public KeysValue withSelectKey1(String selectKey1) {
            return new KeysValue(pop, selectKey1, selectKey2, otherSelectKeys);
        }
        
        /**
         * Construct a new immutable KeysValue object in which selectKey2 is overridden
         */
        public KeysValue withSelectKey2(String selectKey2) {
            return new KeysValue(pop, selectKey1, selectKey2, otherSelectKeys);
        }
        
        /**
         * Construct a new immutable KeysValue object in which otherSelectKeys is overridden
         */
        public KeysValue withOtherSelectKeys(java.util.List<String> otherSelectKeys) {
            return new KeysValue(pop, selectKey1, selectKey2, otherSelectKeys);
        }
    }
    
    public static class KeyValue {
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalPop
         */
        public final java.util.Optional<TraversalPop> pop;
        
        /**
         * @type string
         */
        public final String selectKey;
        
        /**
         * Constructs an immutable KeyValue object
         */
        public KeyValue(java.util.Optional<TraversalPop> pop, String selectKey) {
            this.pop = pop;
            this.selectKey = selectKey;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof KeyValue)) {
                return false;
            }
            KeyValue o = (KeyValue) other;
            return pop.equals(o.pop)
                && selectKey.equals(o.selectKey);
        }
        
        @Override
        public int hashCode() {
            return 2 * pop.hashCode()
                + 3 * selectKey.hashCode();
        }
        
        /**
         * Construct a new immutable KeyValue object in which pop is overridden
         */
        public KeyValue withPop(java.util.Optional<TraversalPop> pop) {
            return new KeyValue(pop, selectKey);
        }
        
        /**
         * Construct a new immutable KeyValue object in which selectKey is overridden
         */
        public KeyValue withSelectKey(String selectKey) {
            return new KeyValue(pop, selectKey);
        }
    }
    
    /**
     * An interface for applying a function to a SelectStep according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Column instance) ;
        
        R visit(Key instance) ;
        
        R visit(Keys instance) ;
        
        R visit(Traversal instance) ;
    }
    
    /**
     * An interface for applying a function to a SelectStep according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(SelectStep instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Column instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Key instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Keys instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Traversal instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalColumn
     */
    public static final class Column extends SelectStep {
        public final TraversalColumn column;
        
        /**
         * Constructs an immutable Column object
         */
        public Column(TraversalColumn column) {
            this.column = column;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Column)) {
                return false;
            }
            Column o = (Column) other;
            return column.equals(o.column);
        }
        
        @Override
        public int hashCode() {
            return 2 * column.hashCode();
        }
    }
    
    public static final class Key extends SelectStep {
        public final KeyValue key;
        
        /**
         * Constructs an immutable Key object
         */
        public Key(KeyValue key) {
            this.key = key;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Key)) {
                return false;
            }
            Key o = (Key) other;
            return key.equals(o.key);
        }
        
        @Override
        public int hashCode() {
            return 2 * key.hashCode();
        }
    }
    
    public static final class Keys extends SelectStep {
        public final KeysValue keys;
        
        /**
         * Constructs an immutable Keys object
         */
        public Keys(KeysValue keys) {
            this.keys = keys;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Keys)) {
                return false;
            }
            Keys o = (Keys) other;
            return keys.equals(o.keys);
        }
        
        @Override
        public int hashCode() {
            return 2 * keys.hashCode();
        }
    }
    
    public static final class Traversal extends SelectStep {
        public final TraversalValue traversal;
        
        /**
         * Constructs an immutable Traversal object
         */
        public Traversal(TraversalValue traversal) {
            this.traversal = traversal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Traversal)) {
                return false;
            }
            Traversal o = (Traversal) other;
            return traversal.equals(o.traversal);
        }
        
        @Override
        public int hashCode() {
            return 2 * traversal.hashCode();
        }
    }
}
