package com.softcell.streaming.flink;

import com.mongodb.BasicDBObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;


public class AccessBasicDBObject extends ScalarFunction {

    public String eval(BasicDBObject basicDBObject, String key) {
        if (basicDBObject.getString(key) != null)
            return basicDBObject.getString(key);
        else return "";
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }
}