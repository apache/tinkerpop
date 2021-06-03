package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A Unicode string that conforms to the syntax defined in RFC 3987
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-IRIs"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 3.2: IRIs"
 *          - id: "http://www.unicode.org/versions/latest"
 *            title: The Unicode Standard
 *          - id: "http://www.ietf.org/rfc/rfc3987.txt"
 *            title: "Internationalized Resource Identifiers (IRIs)"
 * @type string
 */
public class Iri {
    public final String value;
    
    /**
     * Constructs an immutable Iri object
     */
    public Iri(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Iri)) {
            return false;
        }
        Iri o = (Iri) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
