package cn.cmvideo.migu.hive.udf.utils;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.PriorityQueue;

public final class BoundedPriorityQueue<E> {

    @Nonnegative
    private final int maxSize;
    @Nonnull
    private final Comparator<E> comparator;
    @Nonnull
    private final PriorityQueue<E> queue;

    public BoundedPriorityQueue(int size, @Nonnull Comparator<E> comparator) {
        if (size < 1) {
            throw new IllegalArgumentException("Illegal queue size: " + size);
        }
        if (comparator == null) {
            throw new IllegalArgumentException("comparator should not be null");
        }
        this.maxSize = size;
        this.comparator = comparator;
        this.queue = new PriorityQueue<E>(size + 10, comparator);
    }

    public boolean offer(@Nonnull E e) {
        if (e == null) {
            throw new IllegalArgumentException("Null argument is not permitted");
        }
        final int numElem = queue.size();
        if (numElem >= maxSize) {
            E smallest = queue.peek();
            final int cmp = comparator.compare(e, smallest);
            if (cmp < 0) {
                return false;
            }
            queue.poll();
        }
        queue.offer(e);
        return true;
    }

    @Nullable
    public E poll() {
        return queue.poll();
    }

    @Nullable
    public E peek() {
        return queue.peek();
    }

    public int size() {
        return queue.size();
    }

    public void clear() {
        queue.clear();
    }

}