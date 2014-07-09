package com.tinkerpop.gremlin.groovy;

import org.codehaus.groovy.control.customizers.CompilationCustomizer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface CompilerCustomizerProvider {
    public CompilationCustomizer getCompilationCustomizer();
}
