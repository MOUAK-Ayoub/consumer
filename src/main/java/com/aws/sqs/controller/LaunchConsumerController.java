package com.aws.sqs.controller;


import com.aws.sqs.service.SqsConsumerLauncherService;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class LaunchConsumerController {

    private SqsConsumerLauncherService sqsLauncherService;

    public LaunchConsumerController(SqsConsumerLauncherService sqsLauncherService) {
        this.sqsLauncherService = sqsLauncherService;
    }

    @PostConstruct
    public void runAfterStartup() throws InterruptedException {
        System.out.println("Yaaah, I am running........");
        sqsLauncherService.launchSqsConsumer();
    }

}
