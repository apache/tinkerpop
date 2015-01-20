package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Graph;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Default implementation of the {@link Graph.Io} interface which overrides none of the default methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultIo implements Graph.Io {
    private final Graph g;

    public DefaultIo(final Graph g) {
        this.g = g;
    }

    @Override
    public void writeKryo(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            kryoWriter().create().writeGraph(out, g);
        }
    }

    @Override
    public void readKryo(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            kryoReader().create().readGraph(in, g);
        }
    }

    @Override
    public void writeGraphML(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            graphMLWriter().create().writeGraph(out, g);
        }
    }

    @Override
    public void readGraphML(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            graphMLReader().create().readGraph(in, g);
        }
    }

    @Override
    public void writeGraphSON(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            graphSONWriter().create().writeGraph(out, g);
        }
    }

    @Override
    public void readGraphSON(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            graphSONReader().create().readGraph(in, g);
        }
    }
}
