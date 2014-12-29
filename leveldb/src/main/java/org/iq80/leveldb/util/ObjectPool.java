package org.iq80.leveldb.util;

public interface ObjectPool<T>
{
   /**
    * Exclusively acquire an object from the set of available objects in the pool, removing it from
    * the available set. The returned {@link PooledObject} must be {@link PooledObject#close()
    * closed} in order to become available again. Behavior when the pool has no available
    * objects is defined by implementing classes
    */
   public PooledObject<T> acquire();
   

   /**
    * Try to acquire an object from the set of available objects in the pool, removing it from the
    * available set. If no objects are available, {@code null} is returned.
    */
   public PooledObject<T> tryAcquire();

   /**
    * A closeable wrapper around an object belonging to an object pool. A PooledObject is implicitly
    * opened upon calling {@link ObjectPool#acquire()} and must be {@link #close() closed} in
    * order to become available again
    */
   public interface PooledObject<T> extends AutoCloseable
   {
      public T get();
      @Override
      public void close();
   }
}