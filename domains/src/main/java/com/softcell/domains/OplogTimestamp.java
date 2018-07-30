package com.softcell.domains;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OplogTimestamp implements Serializable {
    private static final long serialVersionUID = 1905122041950251101L;
    private long value;
}
