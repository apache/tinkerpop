package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A triple consisting of a subject, predicate, and object
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-triples"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 3.1: Triples"
 */
public class RdfTriple {
    /**
     * The subject of the triple, which is an IRI or a blank node
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.RdfSubjectTerm
     */
    public final RdfSubjectTerm subject;
    
    /**
     * The predicate of the triple, which is an IRI
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.Iri
     */
    public final Iri predicate;
    
    /**
     * The object of the triple, which is an IRI, a literal or a blank node
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.RdfTerm
     */
    public final RdfTerm object;
    
    /**
     * Constructs an immutable RdfTriple object
     */
    public RdfTriple(RdfSubjectTerm subject, Iri predicate, RdfTerm object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RdfTriple)) {
            return false;
        }
        RdfTriple o = (RdfTriple) other;
        return subject.equals(o.subject)
            && predicate.equals(o.predicate)
            && object.equals(o.object);
    }
    
    @Override
    public int hashCode() {
        return 2 * subject.hashCode()
            + 3 * predicate.hashCode()
            + 5 * object.hashCode();
    }
    
    /**
     * Construct a new immutable RdfTriple object in which subject is overridden
     */
    public RdfTriple withSubject(RdfSubjectTerm subject) {
        return new RdfTriple(subject, predicate, object);
    }
    
    /**
     * Construct a new immutable RdfTriple object in which predicate is overridden
     */
    public RdfTriple withPredicate(Iri predicate) {
        return new RdfTriple(subject, predicate, object);
    }
    
    /**
     * Construct a new immutable RdfTriple object in which object is overridden
     */
    public RdfTriple withObject(RdfTerm object) {
        return new RdfTriple(subject, predicate, object);
    }
}
