package org.gbif.pipelines.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.gbif.api.model.common.search.SearchParameter;
import java.util.Date;

@JsonDeserialize(as = EventSearchParameter.class)
public enum EventSearchParameter implements SearchParameter {

    STATE_PROVINCE(String.class),
    DATASET_KEY(String.class),
    YEAR(Integer.class),
    MONTH(Integer.class),
    EVENT_DATE(Date.class),
    EVENT_ID(String.class),
    EVENT_TYPE(String.class),
    PARENT_EVENT_ID(String.class),
    MEASUREMENT_TYPES(String.class),
    SAMPLING_PROTOCOL(String.class);

    private final Class<?> type;
    EventSearchParameter(Class type) {
        this.type = type;
    }

    public Class<?> type() {
        return this.type;
    }
}
