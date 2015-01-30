package com.tinkerpop.gremlin.hadoop.structure.io;

import com.tinkerpop.gremlin.hadoop.structure.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ObjectWritableIterator implements Iterator<KeyValue> {

    private final ObjectWritable key = new ObjectWritable();
    private final ObjectWritable value = new ObjectWritable();
    private boolean available = false;
    private final Queue<SequenceFile.Reader> readers = new LinkedList<>();

    public ObjectWritableIterator(final Configuration configuration, final Path path) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        for (final FileStatus status : fs.listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(new SequenceFile.Reader(fs, status.getPath(), configuration));
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (this.available) {
                return true;
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        return false;
                    if (this.readers.peek().next(this.key, this.value)) {
                        this.available = true;
                        return true;
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public KeyValue next() {
        try {
            if (this.available) {
                this.available = false;
                return new KeyValue<>(this.key.get(), this.value.get());
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        throw FastNoSuchElementException.instance();
                    if (this.readers.peek().next(this.key, this.value)) {
                        return new KeyValue<>(this.key.get(), this.value.get());
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}