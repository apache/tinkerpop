package org.example.org.apache.tinkerpop.gremlin.language.model.literals;

public abstract class GenericLiteral {
    private GenericLiteral() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a GenericLiteral according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(IntegerEsc instance) ;
        
        R visit(FloatEsc instance) ;
        
        R visit(BooleanEsc instance) ;
        
        R visit(StringEsc instance) ;
        
        R visit(Date instance) ;
        
        R visit(Null instance) ;
    }
    
    /**
     * An interface for applying a function to a GenericLiteral according to its variant (subclass). If a visit() method for
     * a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(GenericLiteral instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(IntegerEsc instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(FloatEsc instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(BooleanEsc instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(StringEsc instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Date instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Null instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type integer
     */
    public static final class IntegerEsc extends GenericLiteral {
        public final Integer integer;
        
        /**
         * Constructs an immutable IntegerEsc object
         */
        public IntegerEsc(Integer integer) {
            this.integer = integer;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof IntegerEsc)) {
                return false;
            }
            IntegerEsc o = (IntegerEsc) other;
            return integer.equals(o.integer);
        }
        
        @Override
        public int hashCode() {
            return 2 * integer.hashCode();
        }
    }
    
    /**
     * @type float
     */
    public static final class FloatEsc extends GenericLiteral {
        public final Float floatEsc;
        
        /**
         * Constructs an immutable FloatEsc object
         */
        public FloatEsc(Float floatEsc) {
            this.floatEsc = floatEsc;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FloatEsc)) {
                return false;
            }
            FloatEsc o = (FloatEsc) other;
            return floatEsc.equals(o.floatEsc);
        }
        
        @Override
        public int hashCode() {
            return 2 * floatEsc.hashCode();
        }
    }
    
    /**
     * @type boolean
     */
    public static final class BooleanEsc extends GenericLiteral {
        public final Boolean booleanEsc;
        
        /**
         * Constructs an immutable BooleanEsc object
         */
        public BooleanEsc(Boolean booleanEsc) {
            this.booleanEsc = booleanEsc;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BooleanEsc)) {
                return false;
            }
            BooleanEsc o = (BooleanEsc) other;
            return booleanEsc.equals(o.booleanEsc);
        }
        
        @Override
        public int hashCode() {
            return 2 * booleanEsc.hashCode();
        }
    }
    
    /**
     * @type string
     */
    public static final class StringEsc extends GenericLiteral {
        public final String string;
        
        /**
         * Constructs an immutable StringEsc object
         */
        public StringEsc(String string) {
            this.string = string;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StringEsc)) {
                return false;
            }
            StringEsc o = (StringEsc) other;
            return string.equals(o.string);
        }
        
        @Override
        public int hashCode() {
            return 2 * string.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.DateLiteral
     */
    public static final class Date extends GenericLiteral {
        public final DateLiteral date;
        
        /**
         * Constructs an immutable Date object
         */
        public Date(DateLiteral date) {
            this.date = date;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Date)) {
                return false;
            }
            Date o = (Date) other;
            return date.equals(o.date);
        }
        
        @Override
        public int hashCode() {
            return 2 * date.hashCode();
        }
    }
    
    public static final class Null extends GenericLiteral {
        /**
         * Constructs an immutable Null object
         */
        public Null() {}
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Null)) {
                return false;
            }
            Null o = (Null) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
}
