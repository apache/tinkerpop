package com.tinkerpop.gremlin.groovy;

import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This {@link com.tinkerpop.gremlin.groovy.ImportCustomizerProvider} is empty and comes with no pre-defined imports
 * at all.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EmptyImportCustomizerProvider extends DefaultImportCustomizerProvider {

    /**
     * Utilizes imports defined by the supplied arguments.
     */
    public EmptyImportCustomizerProvider(final Set<String> extraImports, final Set<String> extraStaticImports) {
        this.extraStaticImports.addAll(extraStaticImports);
        this.extraImports.addAll(extraImports);
    }

    /**
     * Utilizes imports defined by the supplied arguments.
     *
     * @param baseCustomizer Imports from this customizer get added to the new one.
     */
    public EmptyImportCustomizerProvider(final ImportCustomizerProvider baseCustomizer,
                                         final Set<String> extraImports, final Set<String> extraStaticImports) {
        this(extraImports, extraStaticImports);
        this.extraImports.addAll(baseCustomizer.getImports());
        this.extraImports.addAll(baseCustomizer.getExtraImports());
        this.extraStaticImports.addAll(baseCustomizer.getStaticImports());
        this.extraStaticImports.addAll(baseCustomizer.getExtraStaticImports());
    }

    @Override
    public CompilationCustomizer getCompilationCustomizer() {
        final ImportCustomizer ic = new ImportCustomizer();

        processImports(ic, extraImports);
        processStaticImports(ic, extraStaticImports);

        return ic;
    }

    @Override
    public Set<String> getImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getStaticImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getAllImports() {
        final Set<String> allImports = new HashSet<>();
        allImports.addAll(extraImports);
        allImports.addAll(extraStaticImports);

        return allImports;
    }
}
