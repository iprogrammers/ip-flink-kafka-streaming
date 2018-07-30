package com.softcell.streaming.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.bson.types.ObjectId;

import java.io.IOException;

public class DocumentIdSerializer extends StdSerializer<ObjectId> {

    public DocumentIdSerializer() {
        this(null);
    }

    public DocumentIdSerializer(Class<ObjectId> t) {
        super(t);
    }

    @Override
    public void serialize(
            ObjectId value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {

//        jgen.writeStartObject();
      /*  jgen.writeNumberField("id", value.id);
        jgen.writeStringField("itemName", value.itemName);
        jgen.writeNumberField("owner", value.owner.id);*/
        jgen.writeString( value.toString());
//        jgen.writeEndObject();
    }
}