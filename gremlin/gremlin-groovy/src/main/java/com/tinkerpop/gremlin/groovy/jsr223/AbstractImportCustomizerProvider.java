package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.blueprints.Direction;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractImportCustomizerProvider implements ImportCustomizerProvider {
    protected static final String DOT_STAR = ".*";
    protected static final String EMPTY_STRING = "";
    protected static final String PERIOD = ".";

    protected final Set<String> extraImports = new HashSet<>();
    protected final Set<String> extraStaticImports = new HashSet<>();

    private static final List<String> imports = new ArrayList<>();
    static {
        imports.add("com.tinkerpop.blueprints.*");
        imports.add("com.tinkerpop.blueprints.computer.*");
        imports.add("com.tinkerpop.blueprints.computer.util.*");
        imports.add("com.tinkerpop.blueprints.query.*");
        imports.add("com.tinkerpop.blueprints.query.util.*");
        imports.add("com.tinkerpop.blueprints.util.*");
        imports.add("com.tinkerpop.blueprints.tinkergraph.*");
    }

    @Override
    public ImportCustomizer getImportCustomizer() {
        final ImportCustomizer ic = new ImportCustomizer();
        for (final String imp : imports) {
            ic.addStarImports(imp.replace(DOT_STAR, EMPTY_STRING));
        }

        ic.addStaticImport(Direction.class.getName(), Direction.OUT.toString());
        ic.addStaticImport(Direction.class.getName(), Direction.IN.toString());
        ic.addStaticImport(Direction.class.getName(), Direction.BOTH.toString());

        importExtras(ic);
        importExtraStatics(ic);

        return ic;
    }

    public Set<String> getExtraImports() {
        return extraImports;
    }

    public Set<String> getExtraStaticImports() {
        return extraStaticImports;
    }

    private void importExtraStatics(final ImportCustomizer ic) {
        for (final String extraStaticImport : extraStaticImports) {
            if (extraStaticImport.endsWith(DOT_STAR)) {
                ic.addStaticStars(extraStaticImport.replace(DOT_STAR, EMPTY_STRING));
            } else {
                final int place = extraStaticImport.lastIndexOf(PERIOD);
                ic.addStaticImport(extraStaticImport.substring(0, place), extraStaticImport.substring(place + 1));
            }
        }
    }

    private void importExtras(final ImportCustomizer ic) {
        for (final String extraImport : extraImports) {
            if (extraImport.endsWith(DOT_STAR)) {
                ic.addStarImports(extraImport.replace(DOT_STAR, EMPTY_STRING));
            } else {
                ic.addImports(extraImport);
            }
        }
    }
}