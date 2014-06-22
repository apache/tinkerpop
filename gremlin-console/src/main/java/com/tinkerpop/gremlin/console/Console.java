package com.tinkerpop.gremlin.console;

import com.tinkerpop.gremlin.groovy.GremlinLoader;
import com.tinkerpop.gremlin.console.commands.RemoteCommand;
import com.tinkerpop.gremlin.console.commands.SubmitCommand;
import com.tinkerpop.gremlin.console.commands.UseCommand;
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import jline.console.history.FileHistory;
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
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Console {

    static {
        // this is necessary so that terminal doesn't lose focus to AWT
        System.setProperty("java.awt.headless", "true");
    }

    private static final String HISTORY_FILE = ".gremlin_groovy_history";
    private static final String STANDARD_INPUT_PROMPT = "gremlin> ";
    public static final String STANDARD_RESULT_PROMPT = "==>";
    private static final String IMPORT_SPACE = "import ";

    private static final IO STANDARD_IO = new IO(System.in, System.out, System.err);
    private static final Groovysh GROOVYSH = new Groovysh();

    public Console(final String initScriptFile) {
        STANDARD_IO.out.println();
        STANDARD_IO.out.println("         \\,,,/");
        STANDARD_IO.out.println("         (o o)");
        STANDARD_IO.out.println("-----oOOo-(3)-oOOo-----");

        final Mediator mediator = new Mediator();
        GROOVYSH.register(new UseCommand(GROOVYSH, mediator));
        GROOVYSH.register(new RemoteCommand(GROOVYSH, mediator));
        GROOVYSH.register(new SubmitCommand(GROOVYSH, mediator));

        // hide output temporarily while imports execute
        GROOVYSH.setResultHook(new NullResultHookClosure(GROOVYSH));

        // add the default imports
        new ConsoleImportCustomizerProvider().getAllImports().stream().map(i -> IMPORT_SPACE + i).forEach(GROOVYSH::execute);

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

		// if plugins were added via :use command (or manually copied to the path) then this will try to find
		// plugins and load them
		ServiceLoader.load(GremlinPlugin.class, GROOVYSH.getInterp().getClassLoader()).forEach(plugin -> {
			if (!mediator.loadedPlugins.containsKey(plugin.getName())) {
				plugin.pluginTo(new ConsolePluginAcceptor(GROOVYSH));
				mediator.loadedPlugins.put(plugin.getName(), plugin);

				STANDARD_IO.out.println("loaded: " + plugin.getName());
			}
		});

        if (initScriptFile != null) initializeShellWithScript(STANDARD_IO, initScriptFile);

        try {
            runner.run();
        } catch (final Throwable e) {
            // System.err.println(e.getMessage());
        } finally {
            try {
                mediator.close().get(3, TimeUnit.SECONDS);
            } catch (Exception ex) {
                // ok if this timesout - just trying to be polite on shutdown
            } finally {
                System.exit(0);
            }
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
        new Console(args.length == 1 ? args[0] : null);
    }
}