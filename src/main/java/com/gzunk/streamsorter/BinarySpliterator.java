package com.gzunk.streamsorter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Binary Spliterator that doesn't hold the elements.
 */
public class BinarySpliterator implements Spliterator<SortableEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(BinarySpliterator.class);
    private BinaryReader binaryReader;

    public BinarySpliterator(Path fileToRead, int maxKeySize, int maxDataSize) {
        this.binaryReader = new BinaryReader(fileToRead, maxKeySize, maxDataSize, false);
    }

    @Override
    public boolean tryAdvance(Consumer<? super SortableEntity> action) {
        return binaryReader.tryAdvance(action);
    }

    @Override
    public Spliterator<SortableEntity> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return 0;
    }

    @Override
    public int characteristics() {
        return 0;
    }
}