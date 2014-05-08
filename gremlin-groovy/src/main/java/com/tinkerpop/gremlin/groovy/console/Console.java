package com.tinkerpop.gremlin.groovy.console;

import com.tinkerpop.gremlin.groovy.GremlinLoader;
import com.tinkerpop.gremlin.groovy.console.commands.GremlinImportCommand;
import com.tinkerpop.gremlin.groovy.console.commands.UseCommand;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import groovy.grape.Grape;
import jline.console.history.FileHistory;
import org.codehaus.groovy.tools.shell.Command;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.codehaus.groovy.tools.shell.InteractiveShellRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Console {

    static {
        // this is necessary so that terminal doesn't lose focus to AWT
        System.setProperty("java.awt.headless", "true");
    }


    private static final Set<String> loadedPlugins = new HashSet<>();

    private static final String HISTORY_FILE = ".gremlin_groovy_history";
    private static final String STANDARD_INPUT_PROMPT = "gremlin> ";
    public static final String STANDARD_RESULT_PROMPT = "==>";
    private static final String IMPORT_SPACE = "import ";
    private static final String DOT_STAR = ".*";

    private static final IO STANDARD_IO = new IO(System.in, System.out, System.err);
    private static final Groovysh GROOVYSH = new Groovysh();

    public Console(final Optional<String> initScriptFile) {
        STANDARD_IO.out.println();
        STANDARD_IO.out.println("         \\,,,/");
        STANDARD_IO.out.println("         (o o)");
        STANDARD_IO.out.println("-----oOOo-(3)-oOOo-----");

        final Command cmd = GROOVYSH.getRegistry().find("import");
        GROOVYSH.getRegistry().remove(cmd);
        GROOVYSH.register(new GremlinImportCommand(GROOVYSH));

        GROOVYSH.register(new UseCommand(GROOVYSH, loadedPlugins));
        GROOVYSH.setResultHook(new NullResultHookClosure(GROOVYSH));

        GROOVYSH.execute(IMPORT_SPACE + Graph.class.getPackage().getName() + DOT_STAR);
        GROOVYSH.execute(IMPORT_SPACE + Traversal.class.getPackage().getName() + DOT_STAR);
        GROOVYSH.execute(IMPORT_SPACE + TinkerGraph.class.getPackage().getName() + DOT_STAR);
        GROOVYSH.execute(IMPORT_SPACE + Grape.class.getCanonicalName());
        GROOVYSH.setResultHook(new ResultHookClosure(GROOVYSH, STANDARD_IO, buildResultPrompt()));

        final InteractiveShellRunner runner = new InteractiveShellRunner(GROOVYSH, new PromptClosure(GROOVYSH, STANDARD_INPUT_PROMPT));
        runner.setErrorHandler(new ErrorHookClosure(runner, STANDARD_IO));
        try {
            final FileHistory history = new FileHistory(new File(System.getProperty("user.home") + "/" + HISTORY_FILE));
            GROOVYSH.setHistory(history);
            runner.setHistory(history);
        } catch (IOException e) {
            STANDARD_IO.err.println("Unable to create history file: " + HISTORY_FILE);
        }

        GremlinLoader.load();
        if (initScriptFile.isPresent())
            initializeShellWithScript(STANDARD_IO, initScriptFile.get());

        try {
            runner.run();
        } catch (final Throwable e) {
            // System.err.println(e.getMessage());
        }
    }

    /**
     * Used by the Gremlin.use() function to send Groovysh instance to the plugin.
     */
    public static Groovysh getGroovysh() {
        return GROOVYSH;
    }

    /**
     * Used by the Gremlin.use() function to send IO instance to the plugin.
     */
    public static IO getStandardIo() {
        return STANDARD_IO;
    }

    private static String buildResultPrompt() {
        final String groovyshellProperty = System.getProperty("gremlin.prompt");
        if (groovyshellProperty != null)
            return groovyshellProperty;

        final String groovyshellEnv = System.getenv("GREMLIN_PROMPT");
        if (groovyshellEnv  != null)
            return  groovyshellEnv;

        return STANDARD_RESULT_PROMPT;
    }

    private void initializeShellWithScript(final IO io, final String initScriptFile) {
        String line = "";
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(initScriptFile), Charset.forName("UTF-8")));
            while ((line = reader.readLine()) != null) {
                GROOVYSH.execute(line);
            }
            reader.close();
        } catch (FileNotFoundException fnfe) {
            io.err.println(String.format("Gremlin initialization file not found at [%s].", initScriptFile));
            System.exit(1);
        } catch (IOException ioe) {
            io.err.println(String.format("Bad line in Gremlin initialization file at [%s].", line));
            System.exit(1);
        }
    }

    public static void main(final String[] args) {
        new Console(args.length == 1 ? Optional.of(args[0]) : Optional.empty());
    }
}