package com.softcell.domains;

import lombok.Data;

@Data
public class FieldConfig {

    private String actualParameter;
    private String collectionName;
    private String expression;
    private Integer index;
    private boolean isExpressionEnabled;
    private boolean isPrimaryKey;
    private boolean isForeignKey;
    private String mappingParameter;
    private String type;

}
