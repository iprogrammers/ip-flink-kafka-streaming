package com.softcell.domains;

import lombok.Data;
import org.bson.Document;

@Data
public class OplogDocument {
    private String host;
    private Document document;
    public OplogDocument(String host, Document document) {
        this.host = host;
        this.document = document;
    }
}
