package com.tinkerpop.gremlin.groovy.jsr223;

import org.codehaus.groovy.control.customizers.ImportCustomizer;

/**
 * Allows customization of the imports used by the GremlinGroovyScriptEngine implementation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ImportCustomizerProvider {
    ImportCustomizer getImportCustomizer();
}
