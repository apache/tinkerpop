package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class TraversalFunction {
    private TraversalFunction() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a TraversalFunction according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Token instance) ;
        
        R visit(Column instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalFunction according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalFunction instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Token instance) {
            return otherwise(instance);
        }
        
        default R visit(Column instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalToken
     */
    public static final class Token extends TraversalFunction {
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
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalColumn
     */
    public static final class Column extends TraversalFunction {
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
            if (!(other instanceof Column)) return false;
            Column o = (Column) other;
            return column.equals(o.column);
        }
        
        @Override
        public int hashCode() {
            return 2 * column.hashCode();
        }
    }
}
