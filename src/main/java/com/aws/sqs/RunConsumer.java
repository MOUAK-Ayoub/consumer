package com.aws.sqs;


import com.amazonaws.ClientConfiguration;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.aws.sqs.consumer.BatchConsumer;
import com.aws.sqs.consumer.Consumer;
import com.aws.sqs.consumer.SimpleConsumer;
import com.aws.sqs.utils.Monitor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;


@SpringBootApplication
public class RunConsumer {

    @Value("${aws.region}")
    String region;


    @Value("${aws.sqs.consumercount}")
    private int consumerCount;

    @Value("${aws.sqs.queuename}")
    private String queueName;


    @Value("${aws.sqs.batchsize}")
    private int batchSize;


    public static void main(String[] args) {

        SpringApplication.run(RunConsumer.class, args);
    }

    @Bean
    public AmazonSQS sqsClient() {


        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(consumerCount);

        final AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.fromName(region))
                .withClientConfiguration(clientConfiguration)
                .build();

        return sqsClient;

    }

    @Bean
    @ConditionalOnProperty(
            value = "aws.sqs.batchsize",
            havingValue = "1",
            matchIfMissing = true)
    List<Consumer> simpleConsumers(AmazonSQS sqsClient, Monitor monitor) {
        List<Consumer> consumers = new ArrayList<>();

        for (int i = 0; i < consumerCount; i++) {
            consumers.add(new SimpleConsumer(sqsClient, queueName,
                    monitor.getConsumedCount(), monitor.getStop()));

        }
        return consumers;
    }

    @Bean
    @ConditionalOnExpression("${aws.sqs.batchsize} > 1 ")
    List<Consumer> batchConsumers(AmazonSQS sqsClient, Monitor monitor) {
        List<Consumer> batchConsumers = new ArrayList<>();

        for (int i = 0; i < consumerCount; i++) {
            batchConsumers.add(new BatchConsumer(sqsClient, queueName, batchSize,
                    monitor.getConsumedCount(), monitor.getStop()));

        }
        return batchConsumers;
    }


}