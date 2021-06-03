package org.example.org.apache.tinkerpop.rdf.rdf11concepts;

/**
 * A BCP 47 language tag
 * 
 * @examples - en
 *           - de
 *           - zh
 * @seeAlso - id: "http://tools.ietf.org/html/bcp47"
 *            title: Tags for Identifying Languages
 * @type string
 */
public class LanguageTag {
    public final String value;
    
    /**
     * Constructs an immutable LanguageTag object
     */
    public LanguageTag(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LanguageTag)) {
            return false;
        }
        LanguageTag o = (LanguageTag) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
