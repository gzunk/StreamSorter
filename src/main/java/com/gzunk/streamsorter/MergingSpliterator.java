package com.gzunk.streamsorter;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MergingSpliterator implements Spliterator<SortableEntity> {

    private final Spliterator<SortableEntity> source1;
    private final Spliterator<SortableEntity> source2;

    private HoldingConsumer source1Holder;
    private HoldingConsumer source2Holder;

    public MergingSpliterator(Spliterator<SortableEntity> source1, Spliterator<SortableEntity> source2) {
        this.source1 = source1;
        this.source2 = source2;
    }

    public Stream<SortableEntity> stream() {
        return StreamSupport.stream(this, false);
    }

    @Override
    public boolean tryAdvance(Consumer<? super SortableEntity> action) {

        source1Holder = HoldingConsumer.getNext(source1Holder, source1);
        source2Holder = HoldingConsumer.getNext(source2Holder, source2);

        if (source1Holder == null && source2Holder == null) {
            return false;
        }

        if (source1Holder == null && source2Holder != null) {
            action.accept(source2Holder.getValue());
            source2Holder = null;
            return true;
        }

        if (source1Holder != null && source2Holder == null) {
            action.accept(source1Holder.getValue());
            source1Holder = null;
            return true;
        }

        int comparison = Integer.signum(source1Holder.getValue().compareTo(source2Holder.getValue()));
        switch (comparison) {
            case 1:
                action.accept(source2Holder.getValue());
                source2Holder = null;
                break;
            default:
                action.accept(source1Holder.getValue());
                source1Holder = null;
        }
        return true;
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
