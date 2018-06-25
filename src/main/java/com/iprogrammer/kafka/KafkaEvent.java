/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.iprogrammer.kafka;

import lombok.Data;
import com.mongodb.BasicDBObject;
import org.bson.BsonTimestamp;


import java.util.Date;

/**
 * The event type used in the {@link }.
 * <p>
 * <p>This is a Java POJO, which Flink recognizes and will allow "by-name" field referencing
 * when keying a {@link org.apache.flink.streaming.api.datastream.DataStream} of such a type.
 * For a demonstration of this, see the code in {@link }.
 */

@Data
public class KafkaEvent {

    private BsonTimestamp ts;
    private String op;
    private String ns;
    private BasicDBObject o;
    private long h;

}
