package org.example.org.apache.tinkerpop.gremlin.language.model.literals;

import org.example.org.apache.tinkerpop.gremlin.language.model.constants.GremlinStringConstant;

public abstract class StringLiteral {
    private StringLiteral() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a StringLiteral according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Empty instance) ;
        
        R visit(NonEmpty instance) ;
        
        R visit(Constant instance) ;
    }
    
    /**
     * An interface for applying a function to a StringLiteral according to its variant (subclass). If a visit() method for
     * a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(StringLiteral instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Empty instance) {
            return otherwise(instance);
        }
        
        default R visit(NonEmpty instance) {
            return otherwise(instance);
        }
        
        default R visit(Constant instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.EmptyStringLiteral
     */
    public static final class Empty extends StringLiteral {
        public final EmptyStringLiteral empty;
        
        /**
         * Constructs an immutable Empty object
         */
        public Empty(EmptyStringLiteral empty) {
            this.empty = empty;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Empty)) return false;
            Empty o = (Empty) other;
            return empty.equals(o.empty);
        }
        
        @Override
        public int hashCode() {
            return 2 * empty.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.NonEmptyStringLiteral
     */
    public static final class NonEmpty extends StringLiteral {
        public final NonEmptyStringLiteral nonEmpty;
        
        /**
         * Constructs an immutable NonEmpty object
         */
        public NonEmpty(NonEmptyStringLiteral nonEmpty) {
            this.nonEmpty = nonEmpty;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NonEmpty)) return false;
            NonEmpty o = (NonEmpty) other;
            return nonEmpty.equals(o.nonEmpty);
        }
        
        @Override
        public int hashCode() {
            return 2 * nonEmpty.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/constants.GremlinStringConstant
     */
    public static final class Constant extends StringLiteral {
        public final GremlinStringConstant constant;
        
        /**
         * Constructs an immutable Constant object
         */
        public Constant(GremlinStringConstant constant) {
            this.constant = constant;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Constant)) return false;
            Constant o = (Constant) other;
            return constant.equals(o.constant);
        }
        
        @Override
        public int hashCode() {
            return 2 * constant.hashCode();
        }
    }
}
