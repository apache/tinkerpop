package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A value such as a string, number, or date
 * 
 * @seeAlso - id: "https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal"
 *            title: "RDF 1.1 Concepts and Abstract Syntax, section 3.2: Literals"
 */
public class RdfLiteral {
    /**
     * A Unicode string which should be in Normal Form C
     * 
     * @seeAlso - id: "http://www.unicode.org/reports/tr15"
     *            title: "TR15, Unicode Normalization Forms"
     * @type string
     */
    public final String lexicalForm;
    
    /**
     * An IRI identifying a datatype that determines how the lexical form maps to a literal value
     * 
     * @type org/apache/tinkerpop/rdf/rdf11concepts.Iri
     */
    public final Iri datatype;
    
    /**
     * An optional language tag, provided only if the datatype of this literal is rdf:langString
     * 
     * @type optional: org/apache/tinkerpop/rdf/rdf11concepts.LanguageTag
     */
    public final java.util.Optional<LanguageTag> languageTag;
    
    /**
     * Constructs an immutable RdfLiteral object
     */
    public RdfLiteral(String lexicalForm, Iri datatype, java.util.Optional<LanguageTag> languageTag) {
        this.lexicalForm = lexicalForm;
        this.datatype = datatype;
        this.languageTag = languageTag;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RdfLiteral)) {
            return false;
        }
        RdfLiteral o = (RdfLiteral) other;
        return lexicalForm.equals(o.lexicalForm)
            && datatype.equals(o.datatype)
            && languageTag.equals(o.languageTag);
    }
    
    @Override
    public int hashCode() {
        return 2 * lexicalForm.hashCode()
            + 3 * datatype.hashCode()
            + 5 * languageTag.hashCode();
    }
    
    /**
     * Construct a new immutable RdfLiteral object in which lexicalForm is overridden
     */
    public RdfLiteral withLexicalForm(String lexicalForm) {
        return new RdfLiteral(lexicalForm, datatype, languageTag);
    }
    
    /**
     * Construct a new immutable RdfLiteral object in which datatype is overridden
     */
    public RdfLiteral withDatatype(Iri datatype) {
        return new RdfLiteral(lexicalForm, datatype, languageTag);
    }
    
    /**
     * Construct a new immutable RdfLiteral object in which languageTag is overridden
     */
    public RdfLiteral withLanguageTag(java.util.Optional<LanguageTag> languageTag) {
        return new RdfLiteral(lexicalForm, datatype, languageTag);
    }
}
