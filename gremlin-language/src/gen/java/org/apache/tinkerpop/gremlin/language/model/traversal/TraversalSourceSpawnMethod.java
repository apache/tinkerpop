package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

import org.example.org.apache.tinkerpop.gremlin.language.model.steps.AddEStep;
import org.example.org.apache.tinkerpop.gremlin.language.model.steps.AddVStep;
import org.example.org.apache.tinkerpop.gremlin.language.model.steps.InjectStep;
import org.example.org.apache.tinkerpop.gremlin.language.model.steps.VStep;

public abstract class TraversalSourceSpawnMethod {
    private TraversalSourceSpawnMethod() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a TraversalSourceSpawnMethod according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(AddE instance) ;
        
        R visit(AddV instance) ;
        
        R visit(E instance) ;
        
        R visit(V instance) ;
        
        R visit(Inject instance) ;
        
        R visit(Io instance) ;
    }
    
    /**
     * An interface for applying a function to a TraversalSourceSpawnMethod according to its variant (subclass). If a
     * visit() method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(TraversalSourceSpawnMethod instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(AddE instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(AddV instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(E instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(V instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Inject instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Io instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AddEStep
     */
    public static final class AddE extends TraversalSourceSpawnMethod {
        public final AddEStep addE;
        
        /**
         * Constructs an immutable AddE object
         */
        public AddE(AddEStep addE) {
            this.addE = addE;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof AddE)) {
                return false;
            }
            AddE o = (AddE) other;
            return addE.equals(o.addE);
        }
        
        @Override
        public int hashCode() {
            return 2 * addE.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.AddVStep
     */
    public static final class AddV extends TraversalSourceSpawnMethod {
        public final AddVStep addV;
        
        /**
         * Constructs an immutable AddV object
         */
        public AddV(AddVStep addV) {
            this.addV = addV;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof AddV)) {
                return false;
            }
            AddV o = (AddV) other;
            return addV.equals(o.addV);
        }
        
        @Override
        public int hashCode() {
            return 2 * addV.hashCode();
        }
    }
    
    public static final class E extends TraversalSourceSpawnMethod {
        /**
         * Constructs an immutable E object
         */
        public E() {}
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof E)) {
                return false;
            }
            E o = (E) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.VStep
     */
    public static final class V extends TraversalSourceSpawnMethod {
        public final VStep v;
        
        /**
         * Constructs an immutable V object
         */
        public V(VStep v) {
            this.v = v;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof V)) {
                return false;
            }
            V o = (V) other;
            return v.equals(o.v);
        }
        
        @Override
        public int hashCode() {
            return 2 * v.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/steps.InjectStep
     */
    public static final class Inject extends TraversalSourceSpawnMethod {
        public final InjectStep inject;
        
        /**
         * Constructs an immutable Inject object
         */
        public Inject(InjectStep inject) {
            this.inject = inject;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Inject)) {
                return false;
            }
            Inject o = (Inject) other;
            return inject.equals(o.inject);
        }
        
        @Override
        public int hashCode() {
            return 2 * inject.hashCode();
        }
    }
    
    public static final class Io extends TraversalSourceSpawnMethod {
        /**
         * Constructs an immutable Io object
         */
        public Io() {}
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Io)) {
                return false;
            }
            Io o = (Io) other;
            return true;
        }
        
        @Override
        public int hashCode() {
            return 0;
        }
    }
}
