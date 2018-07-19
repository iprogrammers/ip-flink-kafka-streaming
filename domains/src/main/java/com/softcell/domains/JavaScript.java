package com.softcell.domains;

import lombok.Data;

import java.util.List;

@Data
public class JavaScript {
    private String script;
    private String methodName;
    private List params;
}
