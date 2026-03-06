package com.crowdstrike.dr.replay.iceberg;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Wraps a CloseableIterable and transforms each element.
 */
public final class TransformCloseableIterable<S, T> implements CloseableIterable<T> {

    private final CloseableIterable<S> source;
    private final Function<S, T> transform;

    public TransformCloseableIterable(CloseableIterable<S> source, Function<S, T> transform) {
        this.source = source;
        this.transform = transform;
    }

    @Override
    public CloseableIterator<T> iterator() {
        Iterator<S> srcIt = source.iterator();
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return srcIt.hasNext();
            }

            @Override
            public T next() {
                return transform.apply(srcIt.next());
            }

            @Override
            public void close() throws IOException {
                source.close();
            }
        };
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
