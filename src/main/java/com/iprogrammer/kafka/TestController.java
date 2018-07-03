package com.iprogrammer.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "test")
public class TestController {

    @Autowired
    MongoTemplate mongoTemplate;

    @PostMapping("insertRecord")
    String insertRecord(@RequestBody CustomerMIS customerMIS) {
        try {
            if (customerMIS.getCustomerMISMaster() != null)
                mongoTemplate.insert(customerMIS.getCustomerMISMaster());
            if (customerMIS.getCustomerMISChild1() != null)
                mongoTemplate.insert(customerMIS.getCustomerMISChild1());
            if (customerMIS.getCustomerMISChild2() != null)
                mongoTemplate.insert(customerMIS.getCustomerMISChild2());
            return "SUCCESS";
        } catch (Exception e) {
            return "Failed: " + e;
        }

    }
}
