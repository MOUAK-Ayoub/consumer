package com.aws.sqs.service;

import com.aws.sqs.consumer.Consumer;
import com.aws.sqs.utils.Monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;


@Service
public class SqsConsumerLauncherService {

    private final static Logger log = LoggerFactory.getLogger(SqsConsumerLauncherService.class);

    @Value("${aws.sqs.runtimeminutes}")
    private int runTimeMinutes;

    final List<Consumer> consumers;


    final Monitor monitor;


    public SqsConsumerLauncherService(
            List<Consumer> consumers,
            Monitor monitor) {
        this.consumers = consumers;
        this.monitor = monitor;
    }


    private void startMonitor() throws InterruptedException {
        // Start the monitor thread.
        monitor.start();

        log.info("Wait on  main thread for: {} minutes", runTimeMinutes);

        // Wait for the specified amount of time then stop.
        Thread.sleep(TimeUnit.MINUTES.toMillis(runTimeMinutes));

        log.info("Resume main thread and stopping the  consumers");
        monitor.getStop().set(true);


        log.info("joining consumers thread");

        consumers.stream().forEach(consumer -> {
            try {
                consumer.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        log.info("joining monitor thread");

        monitor.interrupt();
        monitor.join();
        log.info("Processing finished");
    }

    public void launchSqsConsumer() throws InterruptedException {

        // Start the consumers.

        consumers.stream().forEach(consumer -> consumer.start());

        startMonitor();
    }

}
