package com.tinkerpop.gremlin.console

import com.tinkerpop.gremlin.driver.Cluster
import com.tinkerpop.gremlin.driver.exception.ConnectionException
import com.tinkerpop.gremlin.driver.message.RequestMessage
import com.tinkerpop.gremlin.driver.ser.SerTokens
import com.tinkerpop.gremlin.groovy.AbstractImportCustomizerProvider

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ConsoleImportCustomizerProvider extends AbstractImportCustomizerProvider {
    public ConsoleImportCustomizerProvider() {
        // driver
        extraImports.add(Cluster.class.getPackage().getName() + DOT_STAR);
        extraImports.add(ConnectionException.class.getPackage().getName() + DOT_STAR);
        extraImports.add(RequestMessage.class.getPackage().getName() + DOT_STAR);
        extraImports.add(SerTokens.class.getPackage().getName() + DOT_STAR);
    }
}
