package com.iprogrammer.streaming.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class StreamingConfig {
    private String name;
    private Map<String,List<FieldConfig>> fields;
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm", timezone = "GMT+05:30")
    private Date scheduleAt;
    private String customFunctions;
}
