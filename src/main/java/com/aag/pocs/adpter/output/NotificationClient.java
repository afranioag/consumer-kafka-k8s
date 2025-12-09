package com.aag.pocs.adpter.output;

import com.aag.pocs.adpter.output.request.EmailNotificationRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "notification-client", url = "${notification-service.url}")
public interface NotificationClient {

    @PostMapping("/api/notification")
    void sendNotification(@RequestBody EmailNotificationRequest request);
}
