package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A common prefix for IRIs in the same RDF vocabulary
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#vocabularies"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 1.4: RDF Vocabularies and Namespace IRIs"
 * @type string
 */
public class NamespacePrefix {
    public final String value;
    
    /**
     * Constructs an immutable NamespacePrefix object
     */
    public NamespacePrefix(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NamespacePrefix)) {
            return false;
        }
        NamespacePrefix o = (NamespacePrefix) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
