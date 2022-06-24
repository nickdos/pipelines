package org.gbif.predicate.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Date;
import java.util.UUID;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.vocabulary.*;

@JsonDeserialize(as = OccurrenceSearchParameter.class)
public enum OccurrenceSearchParameter implements SearchParameter {
  DATASET_KEY(String.class),
  YEAR(Integer.class),
  MONTH(Integer.class),
  EVENT_DATE(Date.class),
  EVENT_ID(String.class),
  PARENT_EVENT_ID(String.class),
  SAMPLING_PROTOCOL(String.class),
  LAST_INTERPRETED(Date.class),
  MODIFIED(Date.class),
  DECIMAL_LATITUDE(Double.class),
  DECIMAL_LONGITUDE(Double.class),
  COORDINATE_UNCERTAINTY_IN_METERS(Double.class),
  COUNTRY(Country.class),
  CONTINENT(Continent.class),
  PUBLISHING_COUNTRY(Country.class),
  ELEVATION(Double.class),
  DEPTH(Double.class),
  INSTITUTION_CODE(String.class),
  COLLECTION_CODE(String.class),
  CATALOG_NUMBER(String.class),
  RECORDED_BY(String.class),
  IDENTIFIED_BY(String.class),
  RECORD_NUMBER(String.class),
  BASIS_OF_RECORD(BasisOfRecord.class),
  TAXON_KEY(Integer.class),
  ACCEPTED_TAXON_KEY(Integer.class),
  KINGDOM_KEY(Integer.class),
  PHYLUM_KEY(Integer.class),
  CLASS_KEY(Integer.class),
  ORDER_KEY(Integer.class),
  FAMILY_KEY(Integer.class),
  GENUS_KEY(Integer.class),
  SUBGENUS_KEY(Integer.class),
  SPECIES_KEY(Integer.class),
  SCIENTIFIC_NAME(String.class),
  VERBATIM_SCIENTIFIC_NAME(String.class),
  TAXON_ID(String.class),
  TAXONOMIC_STATUS(TaxonomicStatus.class),
  HAS_COORDINATE(Boolean.class),
  GEOMETRY(String.class),
  GEO_DISTANCE(String.class),
  HAS_GEOSPATIAL_ISSUE(Boolean.class),
  ISSUE(OccurrenceIssue.class),
  TYPE_STATUS(TypeStatus.class),
  MEDIA_TYPE(MediaType.class),
  OCCURRENCE_ID(String.class),
  ESTABLISHMENT_MEANS(String.class),
  DEGREE_OF_ESTABLISHMENT(String.class),
  PATHWAY(String.class),
  REPATRIATED(Boolean.class),
  ORGANISM_ID(String.class),
  STATE_PROVINCE(String.class),
  WATER_BODY(String.class),
  LOCALITY(String.class),
  PROTOCOL(EndpointType.class),
  LICENSE(License.class),
  PUBLISHING_ORG(UUID.class),
  NETWORK_KEY(UUID.class),
  INSTALLATION_KEY(UUID.class),
  HOSTING_ORGANIZATION_KEY(UUID.class),
  CRAWL_ID(Integer.class),
  PROJECT_ID(String.class),
  PROGRAMME(String.class),
  ORGANISM_QUANTITY(Double.class),
  ORGANISM_QUANTITY_TYPE(String.class),
  SAMPLE_SIZE_UNIT(String.class),
  SAMPLE_SIZE_VALUE(Double.class),
  RELATIVE_ORGANISM_QUANTITY(Double.class),
  COLLECTION_KEY(String.class),
  INSTITUTION_KEY(String.class),
  RECORDED_BY_ID(String.class),
  IDENTIFIED_BY_ID(String.class),
  OCCURRENCE_STATUS(OccurrenceStatus.class),
  GADM_GID(String.class),
  GADM_LEVEL_0_GID(String.class),
  GADM_LEVEL_1_GID(String.class),
  GADM_LEVEL_2_GID(String.class),
  GADM_LEVEL_3_GID(String.class),
  LIFE_STAGE(String.class),
  IS_IN_CLUSTER(Boolean.class),
  DWCA_EXTENSION(String.class),
  IUCN_RED_LIST_CATEGORY(String.class),
  DATASET_ID(String.class),
  DATASET_NAME(String.class),
  OTHER_CATALOG_NUMBERS(String.class),
  PREPARATIONS(String.class);

  private final Class<?> type;

  OccurrenceSearchParameter(Class type) {
    this.type = type;
  }

  public Class<?> type() {
    return this.type;
  }
}
