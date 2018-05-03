package com.arcadedb.utility;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Lock manager implementation.
 */
public class PLockManager<RESOURCE, REQUESTER> {
  private final ConcurrentHashMap<RESOURCE, ODistributedLock> lockManager = new ConcurrentHashMap<>(256);

  private class ODistributedLock {
    final REQUESTER      owner;
    final CountDownLatch lock;
    final long           acquiredOn;

    private ODistributedLock(final REQUESTER owner) {
      this.owner = owner;
      this.lock = new CountDownLatch(1);
      this.acquiredOn = System.currentTimeMillis();
    }
  }

  public boolean tryLock(final RESOURCE resource, final REQUESTER requester, final long timeout) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to lock is null");

    final ODistributedLock lock = new ODistributedLock(requester);

    ODistributedLock currentLock = lockManager.putIfAbsent(resource, lock);
    if (currentLock != null) {
      if (currentLock.owner.equals(requester)) {
        // SAME RESOURCE/SERVER, ALREADY LOCKED
        PLogManager.instance().debug(this, "Resource '%s' already locked by requester '%s'", resource, currentLock.owner);
        currentLock = null;
      } else {
        // TRY TO RE-LOCK IT UNTIL TIMEOUT IS EXPIRED
        final long startTime = System.currentTimeMillis();
        do {
          try {
            if (timeout > 0) {
              if (!currentLock.lock.await(timeout, TimeUnit.MILLISECONDS))
                continue;
            } else
              currentLock.lock.await();

            currentLock = lockManager.putIfAbsent(resource, lock);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        } while (currentLock != null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout));
      }
    }

    return currentLock == null;
  }

  public void unlock(final RESOURCE resource, final REQUESTER requester) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to unlock is null");

    final ODistributedLock owner = lockManager.remove(resource);
    if (owner != null) {
      if (!owner.owner.equals(requester)) {
        throw new PLockException(
            "Cannot unlock resource " + resource + " because owner '" + owner.owner + "' <> requester '" + requester + "'");
      }

      // NOTIFY ANY WAITERS
      owner.lock.countDown();
    }
  }

  public void close() {
    for (Iterator<Map.Entry<RESOURCE, ODistributedLock>> it = lockManager.entrySet().iterator(); it.hasNext(); ) {
      final Map.Entry<RESOURCE, ODistributedLock> entry = it.next();
      final ODistributedLock lock = entry.getValue();

      it.remove();

      // NOTIFY ANY WAITERS
      lock.lock.countDown();
    }
  }
}
