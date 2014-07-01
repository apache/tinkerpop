package com.tinkerpop.gremlin.console

import com.tinkerpop.gremlin.groovy.AbstractImportCustomizerProvider
import groovy.sql.Sql
import groovyx.net.http.HTTPBuilder

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ConsoleImportCustomizerProvider extends AbstractImportCustomizerProvider {
    public ConsoleImportCustomizerProvider() {
        // useful groovy bits that are good for the Console
        extraImports.add(Sql.class.getPackage().getName() + DOT_STAR)
        extraImports.add(HTTPBuilder.class.getPackage().getName() + DOT_STAR)
    }
}
