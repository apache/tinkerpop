package com.tinkerpop.gremlin.groovy;

import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.Collections;
import java.util.Set;

/**
 * Provides no imports.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NoImportCustomizerProvider implements ImportCustomizerProvider {
	@Override
	public ImportCustomizer getImportCustomizer() {
		return new ImportCustomizer();
	}

	@Override
	public Set<String> getExtraImports() {
		return Collections.emptySet();
	}

	@Override
	public Set<String> getExtraStaticImports() {
		return Collections.emptySet();
	}

	@Override
	public Set<String> getImports() {
		return Collections.emptySet();
	}
}
