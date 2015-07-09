package org.apache.tinkerpop.gremlin.groovy;

import groovy.transform.CompileStatic;
import groovy.transform.TypeChecked;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

import java.util.HashMap;
import java.util.Map;

/**
 * Injects the {@code CompileStatic} transformer to enable type validation on script execution.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CompileStaticCustomizerProvider implements CompilerCustomizerProvider {

    private final String extensions;

    public CompileStaticCustomizerProvider() {
        this(null);
    }

    public CompileStaticCustomizerProvider(final String extensions) {
        this.extensions = extensions;
    }

    @Override
    public CompilationCustomizer create() {
        final Map<String, Object> compileStaticAnnotationParams = new HashMap<>();
        if (extensions != null && !extensions.isEmpty()) {
            if (extensions.contains(","))
                compileStaticAnnotationParams.put("extensions", extensions.split(","));
            else
                compileStaticAnnotationParams.put("extensions", extensions);
        }

        return new ASTTransformationCustomizer(compileStaticAnnotationParams, CompileStatic.class);
    }
}
