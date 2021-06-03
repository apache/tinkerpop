package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class TraversalSourceSelfMethod {
    private TraversalSourceSelfMethod() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a TraversalSourceSelfMethod according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(WithBulk instance) ;
        
        R visit(WithPath instance) ;
        
        R visit(WithSack instance) ;
        
        R visit(WithSideEffect instance) ;
        
        R visit(WithStrategies instance) ;
        
        R visit(With instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalSourceSelfMethod according to its variant (subclass). If a visit()
     * method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalSourceSelfMethod instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(WithBulk instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithPath instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithSack instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithSideEffect instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithStrategies instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(With instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.WithBulk
     */
    public static final class WithBulk extends TraversalSourceSelfMethod {
        public final WithBulk withBulk;
        
        /**
         * Constructs an immutable WithBulk object
         */
        public WithBulk(WithBulk withBulk) {
            this.withBulk = withBulk;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithBulk)) {
                return false;
            }
            WithBulk o = (WithBulk) other;
            return withBulk.equals(o.withBulk);
        }
        
        @Override
        public int hashCode() {
            return 2 * withBulk.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.WithPath
     */
    public static final class WithPath extends TraversalSourceSelfMethod {
        public final WithPath withPath;
        
        /**
         * Constructs an immutable WithPath object
         */
        public WithPath(WithPath withPath) {
            this.withPath = withPath;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithPath)) {
                return false;
            }
            WithPath o = (WithPath) other;
            return withPath.equals(o.withPath);
        }
        
        @Override
        public int hashCode() {
            return 2 * withPath.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.WithSack
     */
    public static final class WithSack extends TraversalSourceSelfMethod {
        public final WithSack withSack;
        
        /**
         * Constructs an immutable WithSack object
         */
        public WithSack(WithSack withSack) {
            this.withSack = withSack;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithSack)) {
                return false;
            }
            WithSack o = (WithSack) other;
            return withSack.equals(o.withSack);
        }
        
        @Override
        public int hashCode() {
            return 2 * withSack.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.WithSideEffect
     */
    public static final class WithSideEffect extends TraversalSourceSelfMethod {
        public final WithSideEffect withSideEffect;
        
        /**
         * Constructs an immutable WithSideEffect object
         */
        public WithSideEffect(WithSideEffect withSideEffect) {
            this.withSideEffect = withSideEffect;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithSideEffect)) {
                return false;
            }
            WithSideEffect o = (WithSideEffect) other;
            return withSideEffect.equals(o.withSideEffect);
        }
        
        @Override
        public int hashCode() {
            return 2 * withSideEffect.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.WithStrategies
     */
    public static final class WithStrategies extends TraversalSourceSelfMethod {
        public final WithStrategies withStrategies;
        
        /**
         * Constructs an immutable WithStrategies object
         */
        public WithStrategies(WithStrategies withStrategies) {
            this.withStrategies = withStrategies;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithStrategies)) {
                return false;
            }
            WithStrategies o = (WithStrategies) other;
            return withStrategies.equals(o.withStrategies);
        }
        
        @Override
        public int hashCode() {
            return 2 * withStrategies.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.With
     */
    public static final class With extends TraversalSourceSelfMethod {
        public final With with;
        
        /**
         * Constructs an immutable With object
         */
        public With(With with) {
            this.with = with;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof With)) {
                return false;
            }
            With o = (With) other;
            return with.equals(o.with);
        }
        
        @Override
        public int hashCode() {
            return 2 * with.hashCode();
        }
    }
}
