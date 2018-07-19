package com.softcell.rest.controller;


import com.softcell.domains.JavaScript;
import com.softcell.domains.StreamingConfig;
import com.softcell.domains.response.Response;
import com.softcell.rest.service.StreamingConfigService;

import com.softcell.utils.Constant;
import com.softcell.utils.URLEndPoints;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

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

    @GetMapping(URLEndPoints.GET_STREAMING_CONFIG_DETAILS)
    public ResponseEntity<Response> getStreamingCofig(@RequestParam("id") String id) {
        return new ResponseEntity(streamingConfigService.getStreamingConfigDetails(id), HttpStatus.OK);
    }

    @GetMapping(URLEndPoints.GET_STREAMING_CONFIG_LIST)
    public ResponseEntity<Response> getStreamingCofigList() {
        return new ResponseEntity(streamingConfigService.getStreamingConfigList(), HttpStatus.OK);
    }

    @PostMapping(URLEndPoints.UPDATE_STREAMING_CONFIG)
    public ResponseEntity<Response> updateStreamingCofig(@RequestBody StreamingConfig streamingConfig) {
        return new ResponseEntity(streamingConfigService.updateStreamingConfig(streamingConfig), HttpStatus.OK);
    }

    @PostMapping(URLEndPoints.TEST_JAVASCRIPT)
    public ResponseEntity<Response> testJavascript(@RequestBody JavaScript script) {
        return new ResponseEntity(streamingConfigService.testJavascript(script), HttpStatus.OK);
    }


    @PostMapping("test")
    public void testmethod(@RequestBody Map<String,Map> streamingConfigMetadata) {
        System.out.println("request:"+ streamingConfigMetadata);
        //return new ResponseEntity(streamingConfigService.testJavascript(script), HttpStatus.OK);
    }

}
