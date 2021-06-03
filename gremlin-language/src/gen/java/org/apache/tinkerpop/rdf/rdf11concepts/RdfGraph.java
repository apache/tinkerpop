package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A set of RDF triples
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-rdf-graph"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 3: RDF Graphs"
 * @type set: org/apache/tinkerpop/rdf/rdf11concepts.RdfTriple
 */
public class RdfGraph {
    public final java.util.Set<RdfTriple> value;
    
    /**
     * Constructs an immutable RdfGraph object
     */
    public RdfGraph(java.util.Set<RdfTriple> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RdfGraph)) {
            return false;
        }
        RdfGraph o = (RdfGraph) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
