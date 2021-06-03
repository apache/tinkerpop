package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public abstract class TraversalTerminalMethod {
    private TraversalTerminalMethod() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a TraversalTerminalMethod according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Explain instance) ;
        
        R visit(Iterate instance) ;
        
        R visit(HasNext instance) ;
        
        R visit(TryNext instance) ;
        
        R visit(Next instance) ;
        
        R visit(ToList instance) ;
        
        R visit(ToSet instance) ;
        
        R visit(ToBulkSet instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalTerminalMethod according to its variant (subclass). If a visit()
     * method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalTerminalMethod instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Explain instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Iterate instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(HasNext instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(TryNext instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Next instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ToList instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ToSet instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ToBulkSet instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.ExplainMethod
     */
    public static final class Explain extends TraversalTerminalMethod {
        public final ExplainMethod explain;
        
        /**
         * Constructs an immutable Explain object
         */
        public Explain(ExplainMethod explain) {
            this.explain = explain;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Explain)) {
                return false;
            }
            Explain o = (Explain) other;
            return explain.equals(o.explain);
        }
        
        @Override
        public int hashCode() {
            return 2 * explain.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.IterateMethod
     */
    public static final class Iterate extends TraversalTerminalMethod {
        public final IterateMethod iterate;
        
        /**
         * Constructs an immutable Iterate object
         */
        public Iterate(IterateMethod iterate) {
            this.iterate = iterate;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Iterate)) {
                return false;
            }
            Iterate o = (Iterate) other;
            return iterate.equals(o.iterate);
        }
        
        @Override
        public int hashCode() {
            return 2 * iterate.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.HasNextMethod
     */
    public static final class HasNext extends TraversalTerminalMethod {
        public final HasNextMethod hasNext;
        
        /**
         * Constructs an immutable HasNext object
         */
        public HasNext(HasNextMethod hasNext) {
            this.hasNext = hasNext;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HasNext)) {
                return false;
            }
            HasNext o = (HasNext) other;
            return hasNext.equals(o.hasNext);
        }
        
        @Override
        public int hashCode() {
            return 2 * hasNext.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.TryNextMethod
     */
    public static final class TryNext extends TraversalTerminalMethod {
        public final TryNextMethod tryNext;
        
        /**
         * Constructs an immutable TryNext object
         */
        public TryNext(TryNextMethod tryNext) {
            this.tryNext = tryNext;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TryNext)) {
                return false;
            }
            TryNext o = (TryNext) other;
            return tryNext.equals(o.tryNext);
        }
        
        @Override
        public int hashCode() {
            return 2 * tryNext.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.NextMethod
     */
    public static final class Next extends TraversalTerminalMethod {
        public final NextMethod next;
        
        /**
         * Constructs an immutable Next object
         */
        public Next(NextMethod next) {
            this.next = next;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Next)) {
                return false;
            }
            Next o = (Next) other;
            return next.equals(o.next);
        }
        
        @Override
        public int hashCode() {
            return 2 * next.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.ToListMethod
     */
    public static final class ToList extends TraversalTerminalMethod {
        public final ToListMethod toList;
        
        /**
         * Constructs an immutable ToList object
         */
        public ToList(ToListMethod toList) {
            this.toList = toList;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToList)) {
                return false;
            }
            ToList o = (ToList) other;
            return toList.equals(o.toList);
        }
        
        @Override
        public int hashCode() {
            return 2 * toList.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.ToSetMethod
     */
    public static final class ToSet extends TraversalTerminalMethod {
        public final ToSetMethod toSet;
        
        /**
         * Constructs an immutable ToSet object
         */
        public ToSet(ToSetMethod toSet) {
            this.toSet = toSet;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToSet)) {
                return false;
            }
            ToSet o = (ToSet) other;
            return toSet.equals(o.toSet);
        }
        
        @Override
        public int hashCode() {
            return 2 * toSet.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/methods.ToBulkSetMethod
     */
    public static final class ToBulkSet extends TraversalTerminalMethod {
        public final ToBulkSetMethod toBulkSet;
        
        /**
         * Constructs an immutable ToBulkSet object
         */
        public ToBulkSet(ToBulkSetMethod toBulkSet) {
            this.toBulkSet = toBulkSet;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToBulkSet)) {
                return false;
            }
            ToBulkSet o = (ToBulkSet) other;
            return toBulkSet.equals(o.toBulkSet);
        }
        
        @Override
        public int hashCode() {
            return 2 * toBulkSet.hashCode();
        }
    }
}
