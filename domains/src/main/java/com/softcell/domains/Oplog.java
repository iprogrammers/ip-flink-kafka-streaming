package com.softcell.domains;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.mongodb.BasicDBObject;
import lombok.Data;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Oplog implements Serializable{

    private static final long serialVersionUID = 1905122041950251222L;
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
