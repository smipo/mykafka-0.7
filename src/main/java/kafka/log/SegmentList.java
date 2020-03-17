package kafka.log;


import kafka.common.KafkaException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A copy-on-write list implementation that provides consistent views. The view() method
 * provides an immutable sequence representing a consistent state of the list. The user can do
 * iterative operations on this sequence such as binary search without locking all access to the list.
 * Even if the range of the underlying list changes no change will be made to the view
 */
public class SegmentList<T> {


    private AtomicReference<Object[]> contents = new AtomicReference();

    public SegmentList(List<T> logSegments){
        if(logSegments != null && logSegments.size() > 0){
            Object[] obj = new Object[logSegments.size()];
            for(int i = 0;i < logSegments.size();i++){
                obj[i] = logSegments.get(i);
            }
            contents.set(obj);
        }
    }

    /**
     * Append the given items to the end of the list
     */
    public void append(T... ts) {
        Object[] curr = contents.get();
        Object[] updated = new Object[curr.length + ts.length];
        System.arraycopy(curr, 0, updated, 0, curr.length);
        for(int i = 0;i < ts.length;i++)
            updated[curr.length + i] = ts[i];
        contents.set(updated);
    }
    /**
     * Delete the first n items from the list
     */
    public T[]  trunc(int newStart) {
        if(newStart < 0)
            throw new KafkaException("Starting index must be positive.");
        Object[] curr = contents.get();
        if (curr.length > 0) {
            int newLength = Math.max(curr.length - newStart, 0);
            Object[] updated = new Object[newLength];
            System.arraycopy(curr, Math.min(newStart, curr.length - 1), updated, 0, newLength);
            contents.set(updated);
            Object[] deleted  = new Object[newStart];
            System.arraycopy(curr, 0, deleted, 0, curr.length - newLength);
            return (T[])deleted;
        }
        return null;
    }

    /**
     * Delete the items from position (newEnd + 1) until end of list
     */
    public T[]   truncLast(int newEnd) {
        if (newEnd < 0 || newEnd >= contents.get().length)
            throw new KafkaException("Attempt to truncate segment list of length %d to %d.".format(String.valueOf(contents.get().length), newEnd));
        Object[] curr = contents.get();
        if (curr.length > 0) {
            int newLength = newEnd + 1;
            Object[] updated = new Object[newLength];
            System.arraycopy(curr, 0, updated, 0, newLength);
            contents.set(updated);
            Object[] deleted  = new Object[curr.length - newLength];
            System.arraycopy(curr, Math.min(newEnd + 1, curr.length - 1), deleted, 0, curr.length - newLength);
            return (T[])deleted;
        }
        return null;
    }

    /**
     * Get a consistent view of the sequence
     */
    public T[] view(){
        if(contents.get() == null) return null;
        return (T[])contents.get();
    }

    public T last(){
        if(view() == null) return null;
        return view()[view().length - 1];
    }
    /**
     * Nicer toString method
     */
    public String toString(){
        return "SegmentList(%s)".format(view().toString());
    }

}
