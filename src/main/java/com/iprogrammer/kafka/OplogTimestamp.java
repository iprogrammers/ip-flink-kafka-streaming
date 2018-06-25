package com.iprogrammer.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OplogTimestamp {
    private long value;
}
