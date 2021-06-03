package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A node in an RDF graph which is neither an IRI nor a literal
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-blank-nodes"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 3.4: Blank Nodes"
 */
public class BlankNode {
    /**
     * A local identifier for the blank node which may be used in concrete syntaxes
     * 
     * @comments Giving a blank node a string-valued identifier is a pragmatic choice which is intended to support
     * implementations. rdf11-concepts makes clear that such identifiers are not part of the RDF abstract syntax, and should
     * not be used as persistent or portable identifiers for blank nodes.
     * @type string
     */
    public final String identifier;
    
    /**
     * Constructs an immutable BlankNode object
     */
    public BlankNode(String identifier) {
        this.identifier = identifier;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BlankNode)) {
            return false;
        }
        BlankNode o = (BlankNode) other;
        return identifier.equals(o.identifier);
    }
    
    @Override
    public int hashCode() {
        return 2 * identifier.hashCode();
    }
}
