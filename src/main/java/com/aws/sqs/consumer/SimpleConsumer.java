package com.aws.sqs.consumer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// USed the method existing in aws documentation
public class SimpleConsumer extends Consumer {

    private final static Log log = LogFactory.getLog(SimpleConsumer.class);

    final AmazonSQS sqsClient;
    final String queueUrl;
    final AtomicInteger consumedCount;
    final AtomicBoolean stop;
    private final String queueName;

    public SimpleConsumer(AmazonSQS sqsClient, String queueName, AtomicInteger consumedCount,
                    AtomicBoolean stop) {
        this.sqsClient = sqsClient;
        this.queueName = queueName;
        this.queueUrl=this.sqsClient
                .getQueueUrl(new GetQueueUrlRequest(queueName)).getQueueUrl();
        this.consumedCount = consumedCount;
        this.stop = stop;
    }

    
    public void run() {
        try {
            log.info("Begin simple consumer");

            while (!stop.get()) {
                try {
                    final ReceiveMessageResult result = sqsClient
                            .receiveMessage(new
                                    ReceiveMessageRequest(queueUrl));

                    if (!result.getMessages().isEmpty()) {
                        final Message m = result.getMessages().get(0);
                        sqsClient.deleteMessage(new
                                DeleteMessageRequest(queueUrl,
                                m.getReceiptHandle()));
                        consumedCount.incrementAndGet();
                    }
                } catch (AmazonClientException e) {
                    log.error(e.getMessage());
                }
            }
        } catch (AmazonClientException e) {

            log.error("Consumer: " + e.getMessage());
            System.exit(1);
        }
    }
}

