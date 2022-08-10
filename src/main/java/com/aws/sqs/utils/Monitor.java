package com.aws.sqs.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class Monitor extends Thread {

    private final static Log log = LogFactory.getLog(Monitor.class);


    private final AtomicInteger consumedCount;
    private final AtomicBoolean stop;

    public Monitor() {

        consumedCount = new AtomicInteger();
        stop = new AtomicBoolean();
    }

    public void run() {
        try {
            while (!stop.get()) {
                Thread.sleep(1000);
                log.info(" consumed messages = " + consumedCount.get());
            }
        } catch (InterruptedException e) {
            // Allow the thread to exit.

            log.info("Thread exited");
        }
    }


    public AtomicInteger getConsumedCount() {
        return consumedCount;
    }

    public AtomicBoolean getStop() {
        return stop;
    }


}

