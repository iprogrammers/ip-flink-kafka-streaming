package com.iprogrammer.streaming.model;

import lombok.Data;

@Data
public class CustomerMIS {
    private CustomerMISMaster customerMISMaster;
    private CustomerMISChild1 customerMISChild1;
    private CustomerMISChild2 customerMISChild2;
}
