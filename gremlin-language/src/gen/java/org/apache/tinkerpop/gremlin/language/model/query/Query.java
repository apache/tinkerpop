package org.example.org.apache.tinkerpop.gremlin.language.model.query;

import org.example.org.apache.tinkerpop.gremlin.language.model.methods.TraversalTerminalMethod;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.RootTraversal;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalSource;

public abstract class Query {
    private Query() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    public static class WithRootValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.RootTraversal
         */
        public final RootTraversal root;
        
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/methods.TraversalTerminalMethod
         */
        public final java.util.Optional<TraversalTerminalMethod> method;
        
        /**
         * Constructs an immutable WithRootValue object
         */
        public WithRootValue(RootTraversal root, java.util.Optional<TraversalTerminalMethod> method) {
            this.root = root;
            this.method = method;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithRootValue)) {
                return false;
            }
            WithRootValue o = (WithRootValue) other;
            return root.equals(o.root)
                && method.equals(o.method);
        }
        
        @Override
        public int hashCode() {
            return 2 * root.hashCode()
                + 3 * method.hashCode();
        }
        
        /**
         * Construct a new immutable WithRootValue object in which root is overridden
         */
        public WithRootValue withRoot(RootTraversal root) {
            return new WithRootValue(root, method);
        }
        
        /**
         * Construct a new immutable WithRootValue object in which method is overridden
         */
        public WithRootValue withMethod(java.util.Optional<TraversalTerminalMethod> method) {
            return new WithRootValue(root, method);
        }
    }
    
    public static class WithSourceValue {
        /**
         * @type org/apache/tinkerpop/gremlin/language/model/traversal.TraversalSource
         */
        public final TraversalSource source;
        
        /**
         * @type optional: org/apache/tinkerpop/gremlin/language/model/query.TransactionPart
         */
        public final java.util.Optional<TransactionPart> transactionPart;
        
        /**
         * Constructs an immutable WithSourceValue object
         */
        public WithSourceValue(TraversalSource source, java.util.Optional<TransactionPart> transactionPart) {
            this.source = source;
            this.transactionPart = transactionPart;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithSourceValue)) {
                return false;
            }
            WithSourceValue o = (WithSourceValue) other;
            return source.equals(o.source)
                && transactionPart.equals(o.transactionPart);
        }
        
        @Override
        public int hashCode() {
            return 2 * source.hashCode()
                + 3 * transactionPart.hashCode();
        }
        
        /**
         * Construct a new immutable WithSourceValue object in which source is overridden
         */
        public WithSourceValue withSource(TraversalSource source) {
            return new WithSourceValue(source, transactionPart);
        }
        
        /**
         * Construct a new immutable WithSourceValue object in which transactionPart is overridden
         */
        public WithSourceValue withTransactionPart(java.util.Optional<TransactionPart> transactionPart) {
            return new WithSourceValue(source, transactionPart);
        }
    }
    
    /**
     * An interface for applying a function to a Query according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(WithSource instance) ;
        
        R visit(WithRoot instance) ;
        
        R visit(ToString instance) ;
        
        R visit(Empty instance) ;
    }
    
    /**
     * An interface for applying a function to a Query according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(Query instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(WithSource instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(WithRoot instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(ToString instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Empty instance) {
            return otherwise(instance);
        }
    }
    
    public static final class WithSource extends Query {
        public final WithSourceValue withSource;
        
        /**
         * Constructs an immutable WithSource object
         */
        public WithSource(WithSourceValue withSource) {
            this.withSource = withSource;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithSource)) {
                return false;
            }
            WithSource o = (WithSource) other;
            return withSource.equals(o.withSource);
        }
        
        @Override
        public int hashCode() {
            return 2 * withSource.hashCode();
        }
    }
    
    public static final class WithRoot extends Query {
        public final WithRootValue withRoot;
        
        /**
         * Constructs an immutable WithRoot object
         */
        public WithRoot(WithRootValue withRoot) {
            this.withRoot = withRoot;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithRoot)) {
                return false;
            }
            WithRoot o = (WithRoot) other;
            return withRoot.equals(o.withRoot);
        }
        
        @Override
        public int hashCode() {
            return 2 * withRoot.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/query.Query
     */
    public static final class ToString extends Query {
        public final Query toString;
        
        /**
         * Constructs an immutable ToString object
         */
        public ToString(Query toString) {
            this.toString = toString;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ToString)) {
                return false;
            }
            ToString o = (ToString) other;
            return toString.equals(o.toString);
        }
        
        @Override
        public int hashCode() {
            return 2 * toString.hashCode();
        }
    }
    
    public static final class Empty extends Query {
        /**
         * Constructs an immutable Empty object
         */
        public Empty() {}
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Empty)) {
                return false;
            }
            Empty o = (Empty) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
}
