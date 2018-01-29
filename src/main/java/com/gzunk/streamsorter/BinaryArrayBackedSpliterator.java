package com.gzunk.streamsorter;

import java.nio.file.Path;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * Spliterator based on AbstractSpliterator that has an Array backing up the elements. Required because
 * we want to sort this stream.
 */
public class BinaryArrayBackedSpliterator extends Spliterators.AbstractSpliterator<SortableEntity> {

    private BinaryReader binaryReader;

    BinaryArrayBackedSpliterator(Path fileToRead, int maxKeySize, int maxDataSize) {
        super(FileChunker.DEFAULT_CHUNK_SIZE, ~SORTED | NONNULL);
        this.binaryReader = new BinaryReader(fileToRead, maxKeySize, maxDataSize, true);
    }

    @Override
    public boolean tryAdvance(Consumer<? super SortableEntity> action) {
        return binaryReader.tryAdvance(action);
    }
}
