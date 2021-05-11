package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class EdgeLabelVerificationStrategy {
    private EdgeLabelVerificationStrategy() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a EdgeLabelVerificationStrategy according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(ThrowException instance) ;
        
        R visit(LogWarning instance) ;
    }
    
    /**
     * An interface for applying a function to a EdgeLabelVerificationStrategy according to its variant (subclass). If a
     * visit() method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(EdgeLabelVerificationStrategy instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(ThrowException instance) {
            return otherwise(instance);
        }
        
        default R visit(LogWarning instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type boolean
     */
    public static final class ThrowException extends EdgeLabelVerificationStrategy {
        public final Boolean throwException;
        
        /**
         * Constructs an immutable ThrowException object
         */
        public ThrowException(Boolean throwException) {
            this.throwException = throwException;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ThrowException)) return false;
            ThrowException o = (ThrowException) other;
            return throwException.equals(o.throwException);
        }
        
        @Override
        public int hashCode() {
            return 2 * throwException.hashCode();
        }
    }
    
    /**
     * @type boolean
     */
    public static final class LogWarning extends EdgeLabelVerificationStrategy {
        public final Boolean logWarning;
        
        /**
         * Constructs an immutable LogWarning object
         */
        public LogWarning(Boolean logWarning) {
            this.logWarning = logWarning;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof LogWarning)) return false;
            LogWarning o = (LogWarning) other;
            return logWarning.equals(o.logWarning);
        }
        
        @Override
        public int hashCode() {
            return 2 * logWarning.hashCode();
        }
    }
}
