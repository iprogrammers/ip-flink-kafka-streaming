package com.softcell.domains;

import lombok.Data;

@Data
public class CustomerMISTemp {
    private String loanApplicationId;
    private String product;
    private String productCode;
    private String dealerId;
    private String institutionId;
    private String fullName;
    private String customerName;
    private  String branchName;
    private int branchCode;
    private String residenceCity;
    private String residenceState;
    private String dealerCity;
    private String dealerState;
}
