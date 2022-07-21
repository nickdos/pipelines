package org.gbif.predicate.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.gbif.api.model.common.search.SearchParameter;

@JsonDeserialize(as = ALAEventSearchParameter.class)
public enum ALAEventSearchParameter implements SearchParameter {
  DATASET_KEY(String.class),
  PARENT_EVENT_ID(String.class),
  EVENT_ID(String.class),
  EVENT_TYPE(String.class),
  LOCATION_ID(String.class),
  COUNTRY_CODE(String.class),
  YEAR(Integer.class),
  MONTH(Integer.class),
  SAMPLING_PROTOCOL(String.class),
  STATE_PROVINCE(String.class),

  // probably a nicer way to do this, but for now add the darwin core ID version
  datasetKey(String.class),
  parentEventID(String.class),
  eventID(String.class),
  eventType(String.class),
  locationID(String.class),
  countryCode(String.class),
  year(Integer.class),
  month(Integer.class),
  samplingProtocol(String.class),
  stateProvince(String.class);

  private final Class<?> type;

  ALAEventSearchParameter(Class type) {
    this.type = type;
  }

  public Class<?> type() {
    return this.type;
  }
}
