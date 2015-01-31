package com.tinkerpop.gremlin.groovy;

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultImportCustomizerProviderTest {
    static {
        SugarLoader.load();
    }

    @Test
    public void shouldReturnDefaultImports() {
        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider();
        assertImportsInProvider(provider);
    }

    @Test
    public void shouldReturnWithExtraStaticImports() {
        final Set<String> statics = new HashSet<>();
        statics.add("com.test.This.*");
        statics.add("com.test.That.OTHER");
        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider(new HashSet<>(), statics);
        assertImportsInProvider(provider);
    }

    @Test
    public void shouldReturnWithExtraImports() {
        final Set<String> imports = new HashSet<>();
        imports.add("com.test.that.*");
        imports.add("com.test.that.That");
        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider(imports, new HashSet<>());
        assertImportsInProvider(provider);
    }

    @Test
    public void shouldReturnWithBothImportTypes() {
        final Set<String> imports = new HashSet<>();
        imports.add("com.test.that.*");
        imports.add("com.test.that.That");

        final Set<String> statics = new HashSet<>();
        statics.add("com.test.This.*");
        statics.add("com.test.That.OTHER");

        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider(imports, statics);
        assertImportsInProvider(provider);
    }

    private static void assertImportsInProvider(DefaultImportCustomizerProvider provider) {
        final Set<String> allImports = provider.getAllImports();

        final Set<String> imports = provider.getImports();
        final Set<String> staticImports = provider.getStaticImports();
        final Set<String> extraImports = provider.getExtraImports();
        final Set<String> extraStaticImports = provider.getExtraStaticImports();

        assertEquals(imports.size() + staticImports.size() + extraImports.size() + extraStaticImports.size(), allImports.size());
        assertTrue(allImports.containsAll(imports));
        assertTrue(allImports.containsAll(staticImports));
        assertTrue(allImports.containsAll(extraImports));
        assertTrue(allImports.containsAll(extraStaticImports));
    }
}
