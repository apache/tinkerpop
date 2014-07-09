package com.tinkerpop.gremlin.groovy;

import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.kohsuke.groovy.sandbox.GroovyInterceptor;
import org.kohsuke.groovy.sandbox.SandboxTransformer;

import java.util.Arrays;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SecurityCustomizerProvider implements CompilerCustomizerProvider {

    private final List<GroovyInterceptor> interceptors;

    public SecurityCustomizerProvider(final GroovyInterceptor... interceptors) {
        this.interceptors = Arrays.asList(interceptors);
    }

    @Override
    public CompilationCustomizer getCompilationCustomizer() {
        return new SandboxTransformer();
    }

    public void registerInterceptors() {
        interceptors.forEach(GroovyInterceptor::register);
    }

    public void unregisterInterceptors() {
        interceptors.forEach(GroovyInterceptor::unregister);
    }


}
