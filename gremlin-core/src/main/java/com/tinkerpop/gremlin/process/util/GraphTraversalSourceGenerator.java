package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalSourceGenerator {

    private static final Set<String> methodsWithE2AsElementSingle = new HashSet<String>() {{
        add("has");
        add("hasNot");
        add("between");
    }};

    private static final Set<String> methodsWithE2AsElementMulti = new HashSet<String>() {{
        add("sack");
    }};

    public static void main(final String[] args) throws Exception {
        generateSource(args[0], args[1], args[2], args[3], args[4]);
    }

    public static void generateSource(final String traversalToCloneClassName,
                                      final String resultDirectory,
                                      final String stubTraversalClassName,
                                      final String resultTraversalClassName,
                                      final String desiredTraversalReturnClassName) throws Exception {


        final StringBuilder builder = new StringBuilder();
        final Class traversalToCloneClass = Class.forName(traversalToCloneClassName);

        final String mergeClassFile = resultDirectory + "/" + stubTraversalClassName + ".java";
        byte[] encoded = Files.readAllBytes(Paths.get(new File(mergeClassFile).toURI()));
        String sourceCode = new String(encoded);
        int pos = sourceCode.lastIndexOf("}");
        sourceCode = sourceCode.substring(0, pos) + sourceCode.substring(pos + 1);
        sourceCode = sourceCode.replace("interface " + stubTraversalClassName, "public interface " + stubTraversalClassName);
        sourceCode = sourceCode.replace(stubTraversalClassName, resultTraversalClassName);
        sourceCode = "////// THIS CLASS IS AUTO-GENERATED, DO NOT EDIT\n" +
                "////// TO ADD METHODS TO THIS CLASS, EDIT " + stubTraversalClassName + "\n\n" + sourceCode;
        builder.append(sourceCode).append("\n");
        builder.append("\t///////////////////////////////////////////////////////////////////////////////////\n");
        builder.append("\t//// METHODS INHERITED FROM " + traversalToCloneClassName + " ////\n");
        builder.append("\t///////////////////////////////////////////////////////////////////////////////////\n\n");

        final List<Method> methods = Arrays.asList(traversalToCloneClass.getMethods());
        Collections.sort(methods, (a, b) -> (a.getName() + a.getParameterCount() + a.toGenericString()).compareTo((b.getName() + b.getParameterCount() + b.toGenericString())));
        for (final Method method : methods) {
            if (method.getReturnType().equals(GraphTraversal.class) && !Modifier.isStatic(method.getModifiers()) && !method.getName().equals("addStep")) {
                String methodName = sharedToGenericString(method);
                methodName = methodName.replace(GraphTraversal.class.getCanonicalName(), desiredTraversalReturnClassName);
                methodName = methodName.replace(traversalToCloneClass.getCanonicalName() + ".", "");  // needed for ElementTraversal
                methodName = methodName.replace(resultTraversalClassName + ".", "");                  // needed for GraphTraversal
                if (methodsWithE2AsElementSingle.contains(method.getName())) {
                    methodName = methodName.replace("<E2>", "<E2 extends Element>");
                }
                if (methodsWithE2AsElementMulti.contains(method.getName())) {
                    methodName = methodName.replace("<E2,", "<E2 extends Element,");
                }
                final String parameters = Arrays.asList(method.getParameters()).stream().map(p -> p.getName()).collect(Collectors.toList()).toString().replace("[", "").replace("]", "");
                methodName = "\t" + methodName + " {\n\t\treturn (" + desiredTraversalReturnClassName + ") " + traversalToCloneClass.getCanonicalName() + ".super." + method.getName() + "(" + parameters + ");\n\t}\n\n";
                methodName = methodName.replace("$", ".");
                builder.append(methodName);
            }
        }
        builder.append("}");
        PrintWriter out = new PrintWriter(resultDirectory + "/" + resultTraversalClassName + ".java");
        out.write(builder.toString());
        out.flush();
        out.close();
    }

    private static String sharedToGenericString(final Method method) {
        try {
            StringBuilder sb = new StringBuilder();
            Method printModifiersIfNonzeroMethod = Arrays.asList(Executable.class.getDeclaredMethods()).stream().filter(m -> m.getName().equals("printModifiersIfNonzero")).findAny().get();
            Method specificToGenericStringHeaderMethod = Arrays.asList(Executable.class.getDeclaredMethods()).stream().filter(m -> m.getName().equals("specificToGenericStringHeader")).findAny().get();
            printModifiersIfNonzeroMethod.setAccessible(true);
            specificToGenericStringHeaderMethod.setAccessible(true);

            printModifiersIfNonzeroMethod.invoke(method, sb, method.getModifiers(), method.isDefault());

            TypeVariable<?>[] typeparms = method.getTypeParameters();
            if (typeparms.length > 0) {
                boolean first = true;
                sb.append('<');
                for (TypeVariable<?> typeparm : typeparms) {
                    if (!first)
                        sb.append(',');
                    // Class objects can't occur here; no need to test
                    // and call Class.getName().
                    sb.append(typeparm.toString());
                    first = false;
                }
                sb.append("> ");
            }

            specificToGenericStringHeaderMethod.invoke(method, sb);

            sb.append('(');
            Type[] params = method.getGenericParameterTypes();
            for (int j = 0; j < params.length; j++) {
                String param = params[j].getTypeName();
                if (method.isVarArgs() && (j == params.length - 1)) // replace T[] with T...
                    param = param.replaceFirst("\\[\\]$", "...");
                sb.append(param).append(" arg").append(j);
                if (j < (params.length - 1))
                    sb.append(", ");
            }
            sb.append(')');
            Type[] exceptions = method.getGenericExceptionTypes();
            if (exceptions.length > 0) {
                sb.append(" throws ");
                for (int k = 0; k < exceptions.length; k++) {
                    sb.append((exceptions[k] instanceof Class) ?
                            ((Class) exceptions[k]).getName() :
                            exceptions[k].toString());
                    if (k < (exceptions.length - 1))
                        sb.append(',');
                }
            }
            return sb.toString().replace("default transient", "default");
        } catch (Exception e) {
            return "<" + e + ">";
        }
    }
}
