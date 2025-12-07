package com.aag.pocs.adpter.input;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealtController {

    @GetMapping(value = "/api")
    public String api() {
        return "Hello World";
    }
}
