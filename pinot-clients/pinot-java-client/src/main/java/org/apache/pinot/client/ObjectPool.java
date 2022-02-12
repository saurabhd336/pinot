package org.apache.pinot.client;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class ObjectPool<T> {
  private final int poolSize;
  private final BlockingQueue<T> availableObjs;

  public ObjectPool (int poolSize) {
    this.poolSize = poolSize;
    this.availableObjs = new ArrayBlockingQueue<T>(poolSize);
  }

  public void init() throws Exception {
    for (int i = 0; i < poolSize; i++) {
      availableObjs.put(createObject());
    }
  }

  public T lease(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
    return availableObjs.poll(timeout, unit);
  }

  public void release(T obj) throws Exception {
    // It's possible the object has become unusable / must be closed
    // Add a new instance instead to maintain poolSize;
    if (validate(obj)) {
      availableObjs.put(obj);
    } else {
      availableObjs.put(createObject());
    }
  }

  abstract T createObject()
      throws Exception;

  abstract boolean validate(T obj);
}
