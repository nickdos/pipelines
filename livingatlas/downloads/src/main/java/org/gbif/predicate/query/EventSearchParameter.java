package org.gbif.predicate.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Date;
import org.gbif.api.model.common.search.SearchParameter;

@JsonDeserialize(as = EventSearchParameter.class)
public enum EventSearchParameter implements SearchParameter {
  DATASET_KEY(String.class),
  PARENT_EVENT_ID(String.class),
  EVENT_ID(String.class),
  EVENT_TYPE(String.class),
  LOCATION_ID(String.class),
  COUNTRY_CODE(String.class),
  MEASUREMENT_TYPES(String.class),
  YEAR(Integer.class),
  MONTH(Integer.class),
  EVENT_DATE(Date.class),
  SAMPLING_PROTOCOL(String.class),
  STATE_PROVINCE(String.class);

  private final Class<?> type;

  EventSearchParameter(Class type) {
    this.type = type;
  }

  public Class<?> type() {
    return this.type;
  }
}
