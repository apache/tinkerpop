package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class ReservedKeysVerificationStrategy {
    private ReservedKeysVerificationStrategy() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a ReservedKeysVerificationStrategy according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Keys instance) ;
        
        R visit(ThrowException instance) ;
        
        R visit(LogWarning instance) ;
    }
    
    /**
     * An interface for applying a function to a ReservedKeysVerificationStrategy according to its variant (subclass). If a
     * visit() method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(ReservedKeysVerificationStrategy instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(Keys instance) {
            return otherwise(instance);
        }
        
        default R visit(ThrowException instance) {
            return otherwise(instance);
        }
        
        default R visit(LogWarning instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type list: string
     */
    public static final class Keys extends ReservedKeysVerificationStrategy {
        public final java.util.List<String> keys;
        
        /**
         * Constructs an immutable Keys object
         */
        public Keys(java.util.List<String> keys) {
            this.keys = keys;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Keys)) return false;
            Keys o = (Keys) other;
            return keys.equals(o.keys);
        }
        
        @Override
        public int hashCode() {
            return 2 * keys.hashCode();
        }
    }
    
    /**
     * @type boolean
     */
    public static final class ThrowException extends ReservedKeysVerificationStrategy {
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
    public static final class LogWarning extends ReservedKeysVerificationStrategy {
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
