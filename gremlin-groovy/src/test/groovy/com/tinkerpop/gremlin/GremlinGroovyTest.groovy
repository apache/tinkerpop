package com.tinkerpop.gremlin

import com.tinkerpop.gremlin.structure.Graph
import groovy.grape.Grape
import org.codehaus.groovy.jsr223.GroovyCompiledScript
import org.codehaus.groovy.runtime.MetaClassHelper;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl
import org.codehaus.groovy.runtime.MethodClosure
import org.codehaus.groovy.util.ManagedConcurrentValueMap
import org.codehaus.groovy.util.ReferenceBundle
import org.junit.Ignore
import org.junit.Test

import javax.script.Bindings
import javax.script.ScriptContext
import javax.script.SimpleBindings
import java.lang.reflect.Method

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinGroovyTest {
    private ManagedConcurrentValueMap<String, Closure> globalClosures = new ManagedConcurrentValueMap<String, Closure>(ReferenceBundle.getHardBundle());

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
    @Ignore("Just playing")
    public void aTest() {
        System.out.println(Grape.class.getCanonicalName())

        def loader = new GroovyClassLoader()
        def scriptClass = loader.parseClass("import groovy.grape.Grape;import org.apache.commons.configuration.*;import com.tinkerpop.gremlin.structure.*;import com.tinkerpop.gremlin.structure.Compare.*;import com.tinkerpop.gremlin.structure.io.util.*;import com.tinkerpop.gremlin.driver.message.*;import com.tinkerpop.gremlin.process.*;import com.tinkerpop.gremlin.driver.*;import com.tinkerpop.gremlin.driver.exception.*;import com.tinkerpop.gremlin.structure.io.*;import groovy.json.*;import com.tinkerpop.gremlin.structure.strategy.*;import com.tinkerpop.gremlin.structure.io.graphson.*;import com.tinkerpop.gremlin.driver.ser.*;import com.tinkerpop.gremlin.tinkergraph.structure.*;import com.tinkerpop.gremlin.structure.util.*;import com.tinkerpop.gremlin.structure.io.kryo.*;import com.tinkerpop.gremlin.structure.Direction.*;import com.tinkerpop.gremlin.structure.io.graphml.*;import com.tinkerpop.gremlin.algorithm.generator.*;c = new BaseConfiguration();g = TinkerGraph.open(c)")

        def Script scriptObject = (Script) scriptClass.newInstance()

        Method[] methods = scriptClass.getMethods();
        for (Method m : methods) {
            String name = m.getName();
            globalClosures.put(name, new MethodClosure(scriptObject, name));
        }

        MetaClass oldMetaClass = scriptObject.getMetaClass();

        /*
        * We override the MetaClass of this script object so that we can
        * forward calls to global closures (of previous or future "eval" calls)
        * This gives the illusion of working on the same "global" scope.
        */
        scriptObject.setMetaClass(new DelegatingMetaClass(oldMetaClass) {
            @Override
            public Object invokeMethod(Object object, String name, Object args) {
                if (args == null) {
                    return invokeMethod(object, name, MetaClassHelper.EMPTY_ARRAY);
                }
                if (args instanceof Tuple) {
                    return invokeMethod(object, name, ((Tuple) args).toArray());
                }
                if (args instanceof Object[]) {
                    return invokeMethod(object, name, (Object[]) args);
                } else {
                    return invokeMethod(object, name, new Object[1]{args});
                }
            }

            @Override
            public Object invokeMethod(Object object, String name, Object[] args) {
                try {
                    return super.invokeMethod(object, name, args);
                } catch (MissingMethodException mme) {
                    return callGlobal(name, args, ctx);
                }
            }

            @Override
            public Object invokeStaticMethod(Object object, String name, Object[] args) {
                try {
                    return super.invokeStaticMethod(object, name, args);
                } catch (MissingMethodException mme) {
                    return callGlobal(name, args, ctx);
                }
            }
        });

        scriptObject.run();

    }

    private Object callGlobal(String name, Object[] args) {
        return callGlobal(name, args, context);
    }

    private Object callGlobal(String name, Object[] args, ScriptContext ctx) {
        Closure closure = globalClosures.get(name);
        if (closure != null) {
            return closure.call(args);
        } else {
            // Look for closure valued variable in the
            // given ScriptContext. If available, call it.
            Object value = ctx.getAttribute(name);
            if (value instanceof Closure) {
                return ((Closure) value).call(args);
            } // else fall thru..
        }
        throw new MissingMethodException(name, getClass(), args);
    }


}
