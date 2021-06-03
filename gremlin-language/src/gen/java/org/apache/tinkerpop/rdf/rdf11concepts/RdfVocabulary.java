package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A collection of IRIs intended for use in RDF graphs
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#vocabularies"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 1.4: RDF Vocabularies and Namespace IRIs"
 * @type set: org/apache/tinkerpop/rdf/rdf11concepts.Iri
 */
public class RdfVocabulary {
    public final java.util.Set<Iri> value;
    
    /**
     * Constructs an immutable RdfVocabulary object
     */
    public RdfVocabulary(java.util.Set<Iri> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RdfVocabulary)) {
            return false;
        }
        RdfVocabulary o = (RdfVocabulary) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
