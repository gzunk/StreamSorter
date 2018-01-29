package com.gzunk.streamsorter;

import java.util.Spliterator;
import java.util.function.Consumer;

public class HoldingConsumer implements Consumer<SortableEntity> {

    private SortableEntity value;

    @Override
    public void accept(SortableEntity newValue) {
        this.value = newValue;
    }

    public SortableEntity getValue() {
        return value;
    }

    public static HoldingConsumer getNext(final HoldingConsumer existing, final Spliterator<SortableEntity> source) {
        if (existing == null) {
            HoldingConsumer nextElement = new HoldingConsumer();
            if (!source.tryAdvance(nextElement)) {
                nextElement = null;
            }
            return nextElement;
        }
        return existing;
    }
}