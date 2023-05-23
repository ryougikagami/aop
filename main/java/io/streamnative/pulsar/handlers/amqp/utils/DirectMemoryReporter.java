package io.streamnative.pulsar.handlers.amqp.utils;


import io.kubernetes.client.util.Threads;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class DirectMemoryReporter {
    private static final int _1K = 1024;
    private static final String BUSINESS_KEY = "netty_direct_memory";
    private AtomicLong directMemory;
    {
        Field field = null;
        try {
            field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        try {
            directMemory = (AtomicLong) field.get(PlatformDependent.class);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void startReport(){
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(()->{
            while (true) {
                try {
                    int memoryInfo = (int) (directMemory.get() / _1K);
                    log.info("{}:{}k", BUSINESS_KEY, memoryInfo);
                } catch (Exception e) {
                    log.error("mem print err", e);
                }
                Thread.sleep(1000);
            }
        });
    }
}
