package io.streamnative.pulsar.handlers.amqp.collection;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * 功能描述
 *
 * @since 2022-09-30
 */
@Slf4j
public class SyncConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

    public SyncConcurrentHashMap() {
    }

    public SyncConcurrentHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public SyncConcurrentHashMap(Map m) {
        super(m);
    }

    public SyncConcurrentHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public SyncConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
        super(initialCapacity, loadFactor, concurrencyLevel);
    }

    /**
     * value is not null when stat == 0
     */
    private volatile AtomicInteger stat;

    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public void setStat(AtomicInteger stat) {
        this.stat = stat;
    }

    public V getSync(Object key) {
        V value = super.get(key);
        while (Objects.isNull(value)) {
            value = super.get(key);
        }
        return value;
    }

    private V getSyncWithLock(Object key) {
        V v = null;
        if (stat.get() == 0) {
            v = super.get(key);
        } else {
            lock.lock();
            try {
                while (stat.get() != 0) {
                    condition.await();
                }
                v = super.get(key);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                lock.unlock();
            }
        }
        return v;
    }

    @Override
    public V get(Object key) {
        return getSyncWithLock(key);
    }

    private V putSync(K key, V value) {
        lock.lock();
        V v;
        try {
            v = super.put(key, value);
            if (stat.decrementAndGet() == 0) {
                condition.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
        return v;
    }

    @Override
    public V put(K key, V value) {
        return putSync(key, value);
    }

    private V removeSync(Object key) {
        lock.lock();
        V v;
        try {
            stat.getAndIncrement();
            v = super.remove(key);
        }
        finally {
            lock.unlock();
        }
        return v;
    }

    @Override
    public V remove(Object key) {
        return removeSync(key);
    }

    @Override
    public boolean containsKey(Object key) {
        Object value = super.get(key);
        log.info("containsKey ----- value: [{}]", value);
        return value != null;
    }
}
