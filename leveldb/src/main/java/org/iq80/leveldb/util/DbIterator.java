
package org.iq80.leveldb.util;

import org.apache.commons.lang3.tuple.Pair;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public final class DbIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{

   private final OrdinalIterator[] heap;
   private final Comparator<OrdinalIterator> smallerNext, largerPrev;

   private final Comparator<InternalKey> userComparator;

   public DbIterator(MemTableIterator memTableIterator,
         MemTableIterator immutableMemTableIterator,
         List<InternalTableIterator> level0Files,
         List<LevelIterator> levels,
         Comparator<InternalKey> userComparator)
   {
      this.userComparator = userComparator;
      
      ArrayList<OrdinalIterator> ordinalIterators = new ArrayList<>();
      int ordinal = 0;
      if(memTableIterator != null){
         ordinalIterators.add(new OrdinalIterator(ordinal++, memTableIterator));
      }
      
      if(immutableMemTableIterator != null){
         ordinalIterators.add(new OrdinalIterator(ordinal++, immutableMemTableIterator));
      }
      for (InternalTableIterator level0File : level0Files)
      {
         ordinalIterators.add(new OrdinalIterator(ordinal++, level0File));
      }
      for (LevelIterator level : levels)
      {
         ordinalIterators.add(new OrdinalIterator(ordinal++, level));
      }
      
      smallerNext = new SmallerNextElementComparator();
      largerPrev = new LargerPrevElementComparator();
      
      heap = ordinalIterators.toArray(new OrdinalIterator[ordinalIterators.size()]);
      resetHeap();
   }

   @Override
   protected void seekToFirstInternal()
   {
      for(OrdinalIterator ord:heap){
         ord.iterator.seekToFirst();
      }
      resetHeap();
   }

   @Override
   protected void seekToLastInternal()
   {
      seekToEndInternal();
      getPrevElement();
   } 

   @Override
   public void seekToEndInternal()
   {
      for (OrdinalIterator ord : heap)
      {
         ord.iterator.seekToEnd();
      }
      resetHeap();
   }

   @Override
   protected void seekInternal(InternalKey targetKey)
   {
      for(OrdinalIterator ord:heap){
         ord.iterator.seek(targetKey);
      }
      resetHeap();
   }

   private Pair<OrdinalIterator, Integer> getMax(){
      /*
       * forward iteration can take advantage of the heap ordering but reverse iteration cannot,
       * requiring linear search. there were attempts to maintain two parallel heaps, one min-heap
       * and one max-heap each containing the same iterators. however, these proved to be difficult
       * to coordinate. on the bright side, there tends to be only a small number of iterators (less
       * than 10) even when the database contains a substantial number of items (in the millions)
       * (the c++ implementation, as of this writing, uses linear search forwards and backwards)
       */
      OrdinalIterator max = heap[0];
      int maxIndex = 0;

      for(int i = 1; i < heap.length; i++){
         OrdinalIterator ord = heap[i];
         if(largerPrev.compare(ord, max) > 0){
            max = ord;
            maxIndex = i;
         }
      }

      return Pair.of(max, maxIndex);
   }

   @Override
   protected boolean hasNextInternal()
   {
      return heap[0].iterator.hasNext();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      for(OrdinalIterator ord:heap){
         if(ord.iterator.hasPrev()){
            return true;
         }
      }
      return false;
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      Entry<InternalKey, Slice> next = heap[0].iterator.next();
      siftDown(heap, smallerNext, 0, heap[0]);

      return next;
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      Pair<OrdinalIterator, Integer> maxAndIndex = getMax();
      OrdinalIterator ord = maxAndIndex.getLeft();
      int index = maxAndIndex.getRight();
      
      Entry<InternalKey, Slice> prev = ord.iterator.prev();
      siftUp(heap, smallerNext, index, heap[index]);
      siftDown(heap, smallerNext, index, heap[index]);

      return prev;
   }

   @Override
   protected Entry<InternalKey, Slice> peekInternal()
   {
      return heap[0].iterator.peek();
   }

   @Override
   protected Entry<InternalKey, Slice> peekPrevInternal()
   {
      return getMax().getLeft().iterator.peekPrev();
   }

   private void resetHeap(){
      //heapify
      for(int i = (heap.length >>> 1) - 1; i >= 0; i--){
         siftDown(heap, smallerNext, i, heap[i]);
      }
   }

   private <E> void siftDown(E[] queue, Comparator<E> comparator, int k, E x){
        int half = queue.length >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            E c = queue[child];
            int right = child + 1;
            if (right < queue.length &&
                comparator.compare(c, queue[right]) > 0)
                c = queue[child = right];
            if (comparator.compare(x, c) <= 0)
                break;
            queue[k] = c;
            k = child;
        }
        queue[k] = x;
   }

    private <E> void siftUp(E[] queue, Comparator<E> comparator, int k, E x) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            E e = queue[parent];
            if (comparator.compare(x, e) >= 0)
                break;
            queue[k] = e;
            k = parent;
        }
        queue[k] = x;
    }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("DbIterator");
      sb.append("{iterators=").append(Arrays.asList(heap));
      sb.append(", userComparator=").append(userComparator);
      sb.append('}');
      return sb.toString();
   }

   private class OrdinalIterator
   {
      final public ReverseSeekingIterator<InternalKey, Slice> iterator;
      final public int ordinal;

      public OrdinalIterator(int ordinal, ReverseSeekingIterator<InternalKey, Slice> iterator)
      {
         this.ordinal = ordinal;
         this.iterator = iterator;
      }
   }

   protected class SmallerNextElementComparator implements Comparator<OrdinalIterator> 
   {
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         if (o1.iterator.hasNext())
         {
            if (o2.iterator.hasNext())
            {
               //both iterators have a next element
               int result = userComparator.compare(o1.iterator.peek().getKey(), o2.iterator.peek().getKey());
               return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
            }
            return -1; //o2 does not have a next element, consider o1 less than the empty o2
         }
         if(o2.iterator.hasNext()){
            return 1; //o1 does not have a next element, consider o2 less than the empty o1
         }
         return 0; //neither o1 nor o2 have a next element, consider them equals as empty iterators in this direction
      }
   }

   protected class LargerPrevElementComparator implements Comparator<OrdinalIterator>
   {
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         if (o1.iterator.hasPrev())
         {
            if (o2.iterator.hasPrev())
            {
               int result = userComparator.compare(o1.iterator.peekPrev().getKey(), o2.iterator.peekPrev().getKey());
               return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
            }
            return 1; //if o2 has no prev, return o1 as larger
         }
         if(o2.iterator.hasPrev()){
            return -1; //if o1 has no prev, return o2 as larger
         }
         return 0;
      }
   }
}