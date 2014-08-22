package com.tinkerpop.gremlin.groovy;

import java.util.HashSet;
import java.util.Set;

/**
 * Grabs the standard Gremlin core classes and allows additional imports to be added.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultImportCustomizerProvider extends AbstractImportCustomizerProvider {
    private static Set<String> staticExtraImports = new HashSet<>();
    private static Set<String> staticExtraStaticImports = new HashSet<>();

    /**
     * Utilizes imports defined statically by initializeStatically().
     */
    public DefaultImportCustomizerProvider() {
        this.extraImports.addAll(staticExtraImports);
        this.extraStaticImports.addAll(staticExtraStaticImports);
    }

    /**
     * Utilizes imports defined by the supplied arguments.  Those imports defined statically through
     * initializeStatically() are ignored.
     */
    public DefaultImportCustomizerProvider(final Set<String> extraImports, final Set<String> extraStaticImports) {
        this.extraStaticImports.addAll(extraStaticImports);
        this.extraImports.addAll(extraImports);
    }

    /**
     * Utilizes imports defined by the supplied arguments.  Those imports defined statically through
     * initializeStatically() are ignored.
     *
     * @param baseCustomizer Imports from this customizer get added to the new one
     */
    public DefaultImportCustomizerProvider(final ImportCustomizerProvider baseCustomizer,
                                           final Set<String> extraImports, final Set<String> extraStaticImports) {
        this(extraImports, extraStaticImports);
        this.extraImports.addAll(baseCustomizer.getExtraImports());
        this.extraStaticImports.addAll(baseCustomizer.getExtraStaticImports());
    }

    /**
     * Allows imports to defined globally and statically. This method must be called prior to initialization of
     * a ScriptEngine instance through the ScriptEngineFactory.
     */
    public static void initializeStatically(final Set<String> extraImports, final Set<String> extraStaticImports) {
        if (extraImports != null) {
            staticExtraImports = extraImports;
        }

        if (extraStaticImports != null) {
            staticExtraStaticImports = extraStaticImports;
        }
    }
}