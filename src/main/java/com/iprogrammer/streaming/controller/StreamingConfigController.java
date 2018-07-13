package com.iprogrammer.streaming.controller;

import com.iprogrammer.streaming.model.StreamingConfig;
import com.iprogrammer.streaming.model.URLEndPoints;
import com.iprogrammer.streaming.model.response.Response;
import com.iprogrammer.streaming.service.StreamingConfigService;
import com.iprogrammer.streaming.utils.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = Constant.REPORTS)
public class StreamingConfigController {

    @Autowired
    StreamingConfigService streamingConfigService;

    @GetMapping(URLEndPoints.GET_COLLECTION_NAMES)
    public ResponseEntity<Response> getCollectionNames() {
        return new ResponseEntity(streamingConfigService.getCollectionNames(), HttpStatus.OK);
    }

    @GetMapping(URLEndPoints.GET_COLLECTION_FIELDS)
    public ResponseEntity<Response> getFieldsFromCollection(@RequestParam(value = "collectionName") String collectionName) {
        return new ResponseEntity(streamingConfigService.getFieldsFromCollection(collectionName), HttpStatus.OK);
    }

    @PostMapping(URLEndPoints.CREATE_STREAMING_CONFIG)
    public ResponseEntity<Response> createStreamingCofig(@RequestBody StreamingConfig streamingConfig) {
        return new ResponseEntity(streamingConfigService.createStreamingConfig(streamingConfig), HttpStatus.OK);
    }

}
