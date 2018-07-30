package com.softcell.domains.request;

import lombok.Data;

@Data
public class StreamingConfigRequest {
    private int limit;
    private int startIndex;
    private String searchString;
    private String sortBy;
    private String sortOrder;
}
