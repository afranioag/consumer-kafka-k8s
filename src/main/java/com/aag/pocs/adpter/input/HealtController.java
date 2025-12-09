package com.aag.pocs.adpter.input;

import com.aag.pocs.adpter.output.NotificationClient;
import com.aag.pocs.adpter.output.request.EmailNotificationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/api")
@RequiredArgsConstructor
public class HealtController {

    private final NotificationClient client;

    @GetMapping
    public String api() {
        return "Hello World";
    }

   @PostMapping
    public ResponseEntity<String> sendNotification(@RequestBody EmailNotificationRequest request) {
        client.sendNotification(request);
        return ResponseEntity.ok("Notification sent successfully");
    }
}
