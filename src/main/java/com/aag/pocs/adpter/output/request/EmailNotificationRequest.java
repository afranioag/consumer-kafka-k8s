package com.aag.pocs.adpter.output.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EmailNotificationRequest {
    private String emailDestin;
    private String emailSend;
    private String message;
    private LocalDateTime dateSend;

}
