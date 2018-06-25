package com.iprogrammer.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(value = "reports")
public class TestController {

    @Autowired
    Test test;

    @GetMapping(value = "tail")
    List getAdminReportList() {
        test.test();

        return Collections.emptyList();
    }
}
