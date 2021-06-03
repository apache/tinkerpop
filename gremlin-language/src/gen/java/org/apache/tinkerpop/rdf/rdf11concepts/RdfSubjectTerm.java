package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * An RDF term which may appear in the subject position of a triple, i.e. an IRI or a blank node
 * 
 * @comments The concept of an "RDF subject" is not directly defined in rdf11-concepts; it is provided here as a
 * convenience.
 */
public abstract class RdfSubjectTerm {
    private RdfSubjectTerm() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a RdfSubjectTerm according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Iri instance) ;
        
        R visit(BlankNode instance) ;
    }
    
    /**
     * An interface for applying a function to a RdfSubjectTerm according to its variant (subclass). If a visit() method for
     * a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(RdfSubjectTerm instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Iri instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(BlankNode instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * An IRI
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.Iri
     */
    public static final class Iri extends RdfSubjectTerm {
        public final Iri iri;
        
        /**
         * Constructs an immutable Iri object
         */
        public Iri(Iri iri) {
            this.iri = iri;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Iri)) {
                return false;
            }
            Iri o = (Iri) other;
            return iri.equals(o.iri);
        }
        
        @Override
        public int hashCode() {
            return 2 * iri.hashCode();
        }
    }
    
    /**
     * A blank node
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.BlankNode
     */
    public static final class BlankNode extends RdfSubjectTerm {
        public final BlankNode blankNode;
        
        /**
         * Constructs an immutable BlankNode object
         */
        public BlankNode(BlankNode blankNode) {
            this.blankNode = blankNode;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BlankNode)) {
                return false;
            }
            BlankNode o = (BlankNode) other;
            return blankNode.equals(o.blankNode);
        }
        
        @Override
        public int hashCode() {
            return 2 * blankNode.hashCode();
        }
    }
}
