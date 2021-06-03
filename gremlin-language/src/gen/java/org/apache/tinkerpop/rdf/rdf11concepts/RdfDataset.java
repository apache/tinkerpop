package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A collection of RDF graphs, consisting of a default graph and a set of named graphs
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-dataset"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 4: RDF Datasets"
 */
public class RdfDataset {
    /**
     * The single default (unnamed) graph of the RDF dataset
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.RdfGraph
     */
    public final RdfGraph defaultGraph;
    
    /**
     * The named graphs of the RDF dataset, as a map from unique graph names to graphs
     * 
     * @type map:
     *         keys: org/apache/tinkerpop/rdf/rdf11concepts.RdfSubjectTerm
     *         values: org/apache/tinkerpop/rdf/rdf11concepts.RdfGraph
     */
    public final java.util.Map<RdfSubjectTerm, RdfGraph> namedGraphs;
    
    /**
     * Constructs an immutable RdfDataset object
     */
    public RdfDataset(RdfGraph defaultGraph, java.util.Map<RdfSubjectTerm, RdfGraph> namedGraphs) {
        this.defaultGraph = defaultGraph;
        this.namedGraphs = namedGraphs;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RdfDataset)) {
            return false;
        }
        RdfDataset o = (RdfDataset) other;
        return defaultGraph.equals(o.defaultGraph)
            && namedGraphs.equals(o.namedGraphs);
    }
    
    @Override
    public int hashCode() {
        return 2 * defaultGraph.hashCode()
            + 3 * namedGraphs.hashCode();
    }
    
    /**
     * Construct a new immutable RdfDataset object in which defaultGraph is overridden
     */
    public RdfDataset withDefaultGraph(RdfGraph defaultGraph) {
        return new RdfDataset(defaultGraph, namedGraphs);
    }
    
    /**
     * Construct a new immutable RdfDataset object in which namedGraphs is overridden
     */
    public RdfDataset withNamedGraphs(java.util.Map<RdfSubjectTerm, RdfGraph> namedGraphs) {
        return new RdfDataset(defaultGraph, namedGraphs);
    }
}
