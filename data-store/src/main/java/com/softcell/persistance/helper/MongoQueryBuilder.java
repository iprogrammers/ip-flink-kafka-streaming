package com.softcell.persistance.helper;

import com.softcell.domains.request.StreamingConfigRequest;
import com.softcell.utils.Constant;
import com.softcell.utils.FieldName;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
public class MongoQueryBuilder {

    public Query buildStreamingConfigListQuery(StreamingConfigRequest streamingConfigRequest) {

        Query query = new Query();

        if (StringUtils.isNotEmpty(streamingConfigRequest.getSortOrder()) && StringUtils.isNotEmpty(streamingConfigRequest.getSortBy())) {
            Sort.Direction sortOrder = streamingConfigRequest.getSortOrder().equalsIgnoreCase("asc") ? Sort.Direction.ASC : Sort.Direction.DESC;
            query.with(new Sort(sortOrder, streamingConfigRequest.getSortBy()));
        } else {
            query.with(new Sort(Sort.Direction.DESC, FieldName.UPDATED_AT));
        }

        query.fields().include(FieldName.DOCUMENT_ID);
        query.fields().include(FieldName.SCHEDULE_AT);
        query.fields().include(FieldName.UPDATED_AT);
        query.fields().include(FieldName.STATUS);
        query.fields().include(FieldName.JOB_STATUS);
        query.fields().include(FieldName.NAME);

        if (streamingConfigRequest.getSearchString() != null) {
            query.addCriteria(Criteria
                    .where(FieldName.NAME)
                    .regex(streamingConfigRequest.getSearchString(), "i"));
        }

        int limit = streamingConfigRequest.getLimit() != 0 ? streamingConfigRequest.getLimit() : Constant.DEFAULT_LIMIT;

        long startIndex = streamingConfigRequest.getStartIndex() != 0 ? streamingConfigRequest.getStartIndex() : 0;

        query.limit(limit);

        query.skip(limit * startIndex);

        return query;
    }
}
