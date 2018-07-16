package com.softcell.streaming.model;

import lombok.Data;

@Data
public class CustomerMISMaster {
    private String loanApplicationId;
    private String product;
    private String productCode;
    private String dealerId;
    private String institutionId;
    private String fullName;
    private String customerName;
}
