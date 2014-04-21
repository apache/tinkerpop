package com.tinkerpop.gremlin

import com.tinkerpop.gremlin.structure.Graph
import groovy.grape.Grape
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl
import org.junit.Ignore
import org.junit.Test

import javax.script.Bindings
import javax.script.SimpleBindings

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinGroovyTest {
    @Test
    @Ignore("Just playing")
    public void shouldEval() {
        def roots = new String[1]
        roots[0] = "./"
        GroovyScriptEngine gse = new GroovyScriptEngine(roots)
        gse.loadScriptByName("test.groovy")

        /*
        Grape.grab(group:'org.apache.commons', module:'commons-math3', version:'3.2', classLoader:gse.groovyClassLoader)
        def importCustomizer = new ImportCustomizer()
        importCustomizer.addImports("org.apache.commons.math3.util.FastMath")
        def compilerConfiguration = new CompilerConfiguration()
        compilerConfiguration.addCompilationCustomizers(importCustomizer)
        */

        Bindings binding = new SimpleBindings()
        binding.put("foo", new Integer(2))
        binding.put("gcl", gse.groovyClassLoader)

        def se = new GroovyScriptEngineImpl(gse.groovyClassLoader)
        println se.eval("""
            import groovy.grape.Grape
            Grape.grab(group:'org.apache.commons', module:'commons-math3', version:'3.2', classLoader:gcl)
            import org.apache.commons.math3.util.FastMath
            Grape.getInstance().listDependencies(gcl).each{println it}
            z = Worker.sum(100, foo)
            Worker.sum(FastMath.PI, z)
        """, binding)

        /*
        GroovyShell shell = new GroovyShell(gse.groovyClassLoader, binding, compilerConfiguration)
        println shell.evaluate("""
            import groovy.grape.Grape
            Grape.getInstance().listDependencies(gcl).each{println it}
            z = Worker.sum(100, foo)
            Worker.sum(FastMath.PI, z)
        """)
        */
    }

    @Test
    public void aTest() {
        System.out.println(Grape.class.getCanonicalName())
    }

}
