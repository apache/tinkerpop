package com.tinkerpop.gremlin.groovy;

import java.util.Set;

/**
 * Allows customization of the imports used by the GremlinGroovyScriptEngine implementation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ImportCustomizerProvider extends CompilerCustomizerProvider {

    Set<String> getExtraImports();

    Set<String> getExtraStaticImports();

    Set<String> getImports();
}
