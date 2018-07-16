package com.softcell.domains;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

public class JsonJodaDateTimeSerializer extends JsonSerializer<DateTime> {

    private static DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis();

    @Override
    public void serialize(DateTime value, JsonGenerator gen, SerializerProvider arg2)
            throws IOException {

        gen.writeString(formatter.print(value));
    }

}
