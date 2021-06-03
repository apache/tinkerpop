package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * An IRI, literal, or blank node
 * 
 * @comments Whereas the "nodes" of a graph are all of that graph's subjects and objects, RDF terms are the set of all
 * possible IRIs, literals, and blank nodes.
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-triples"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 3.1: Triples"
 */
public abstract class RdfTerm {
    private RdfTerm() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a RdfTerm according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Iri instance) ;
        
        R visit(Literal instance) ;
        
        R visit(BlankNode instance) ;
    }
    
    /**
     * An interface for applying a function to a RdfTerm according to its variant (subclass). If a visit() method for a
     * particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(RdfTerm instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Iri instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Literal instance) {
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
    public static final class Iri extends RdfTerm {
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
     * A literal
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.RdfLiteral
     */
    public static final class Literal extends RdfTerm {
        public final RdfLiteral literal;
        
        /**
         * Constructs an immutable Literal object
         */
        public Literal(RdfLiteral literal) {
            this.literal = literal;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Literal)) {
                return false;
            }
            Literal o = (Literal) other;
            return literal.equals(o.literal);
        }
        
        @Override
        public int hashCode() {
            return 2 * literal.hashCode();
        }
    }
    
    /**
     * A blank node
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.BlankNode
     */
    public static final class BlankNode extends RdfTerm {
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
