package com.softcell.streaming.model;

import lombok.Data;

@Data
public class FieldConfig {

    private String actualParameter;
    private String collectionName;
    private String collectionPrefix;
    private String displayParameter;
    private String expression;
    private int index;
    private boolean isExpressionEnabled;
    private boolean isPrimaryKey;
    private boolean isForeignKey;
    private String mappingParameter;
    private String type;

}
