package com.iprogrammer.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.mongodb.BasicDBObject;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Oplog {

    private OplogTimestamp ts;
    private String op;
    private String primaryKey;
    private String foreignKey;
    private BasicDBObject o;
    private int t;
    private int v;
    private String ns;
    private int partitionId;
}
