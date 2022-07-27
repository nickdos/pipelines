package au.org.ala.pipelines.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.*;
import java.util.stream.Collectors;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.*;
import org.gbif.pipelines.io.avro.json.VocabularyConcept;

@Slf4j
@SuperBuilder
public class ALAParentJsonConverter {

  protected final ALAMetadataRecord metadata;
  protected final IdentifierRecord identifier;
  protected final EventCoreRecord eventCore;
  protected final TemporalRecord temporal;
  protected final LocationRecord location;
  protected final MultimediaRecord multimedia;
  protected final ExtendedRecord verbatim;
  protected final DerivedMetadataRecord derivedMetadata;
  protected OccurrenceJsonRecord occurrenceJsonRecord;
  protected MeasurementOrFactRecord measurementOrFactRecord;
  private DenormalisedEvent denormalisedEvent;

  private String[] samplingProtocols;

  public ParentJsonRecord convertToParent() {
    return (occurrenceJsonRecord != null) ? convertToParentOccurrence() : convertToParentEvent();
  }

  public String toJson() {
    return convertToParent().toString();
  }

  /** Converts to parent record based on an event record. */
  private ParentJsonRecord convertToParentEvent() {
    return convertToParentRecord()
        .setType("event")
        .setJoinRecordBuilder(JoinRecord.newBuilder().setName("event"))
        .setEventBuilder(convertToEvent())
        .build();
  }

  /** Converts to a parent record based on an occurrence record. */
  private ParentJsonRecord convertToParentOccurrence() {
    return ParentJsonRecord.newBuilder()
        .setType("occurrence")
        .setId(occurrenceJsonRecord.getId())
        .setInternalId(
            HashConverter.getSha1(
                metadata.getDataResourceUid(),
                occurrenceJsonRecord.getVerbatim().getParentCoreId(),
                occurrenceJsonRecord.getOccurrenceId()))
        .setJoinRecordBuilder(
            JoinRecord.newBuilder()
                .setName("occurrence")
                .setParent(
                    HashConverter.getSha1(
                        metadata.getDataResourceUid(),
                        occurrenceJsonRecord.getVerbatim().getParentCoreId())))
        .setOccurrence(occurrenceJsonRecord)
        .setMetadataBuilder(mapMetadataJsonRecord())
        .build();
  }

  /** Converts to a parent record */
  private ParentJsonRecord.Builder convertToParentRecord() {
    ParentJsonRecord.Builder builder =
        ParentJsonRecord.newBuilder()
            .setId(verbatim.getId())
            .setInternalId(identifier.getInternalId())
            .setUniqueKey(identifier.getUniqueKey())
            .setMetadataBuilder(mapMetadataJsonRecord());

    mapCreated(builder);
    mapDerivedMetadata(builder);

    JsonConverter.convertToDate(identifier.getFirstLoaded()).ifPresent(builder::setFirstLoaded);

    return builder;
  }

  private EventJsonRecord.Builder convertToEvent() {

    EventJsonRecord.Builder builder = EventJsonRecord.newBuilder();

    builder.setId(verbatim.getId());

    mapEventCoreRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);
    mapTaxonRecord(builder);
    mapMeasurementOrFactRecord(builder);
    mapDenormalisedEvent(builder);

    // synthesize a locationID if one isnt provided
    if (builder.getLocationID() == null
        && location.getDecimalLatitude() != null
        && location.getDecimalLongitude() != null) {
      builder.setLocationID(
          Math.abs(location.getDecimalLatitude())
              + (location.getDecimalLatitude() > 0 ? "N" : "S")
              + ", "
              + Math.abs(location.getDecimalLongitude())
              + (location.getDecimalLongitude() > 0 ? "E" : "W"));
    }

    // set the event type to the raw value, if the vocab not matched
    if (builder.getEventType() == null) {
      Optional<String> eventType = extractOptValue(verbatim, GbifTerm.eventType);
      if (eventType.isPresent()) {
        VocabularyConcept eventTypeVoc =
            VocabularyConcept.newBuilder()
                .setLineage(Arrays.asList("Event", eventType.get()))
                .setConcept(eventType.get())
                .build();
        builder.setEventType(eventTypeVoc);
        builder.setEventHierarchyJoined(eventTypeVoc.getConcept());
      }
    }

    // populate with the type for this event at least
    if (builder.getEventTypeHierarchy() == null || builder.getEventTypeHierarchy().isEmpty()) {
      List<String> eventTypeHierarchy = new ArrayList<>();
      if (builder.getEventType() != null) {
        eventTypeHierarchy.add(builder.getEventType().getConcept());
      }
      builder.setEventTypeHierarchy(eventTypeHierarchy);
      if (builder.getEventType() != null) {
        builder.setEventTypeHierarchyJoined(builder.getEventType().getConcept());
      }
    }

    // retrieve sampling protocol from occurrences, if nothing supplied in events
    if (builder.getSamplingProtocol() == null && builder.getSamplingProtocol().isEmpty()) {
      Optional<List<String>> samplingProtocols =
          Optional.of(verbatim.getExtensions())
              .map(exts -> exts.get(DwcTerm.Occurrence.qualifiedName()))
              .map(ext -> extractValuesFromExtension(ext, DwcTerm.samplingProtocol));
      builder.setSamplingProtocol(samplingProtocols.orElse(new ArrayList<>()));
    }

    return builder;
  }

  public static List<String> extractValuesFromExtension(
      List<Map<String, String>> extension, DwcTerm term) {
    return extension.stream()
        .map(x -> x.getOrDefault(term.qualifiedName(), null))
        .filter(x -> x != null)
        .distinct()
        .collect(Collectors.toList());
  }

  private MetadataJsonRecord.Builder mapMetadataJsonRecord() {
    return MetadataJsonRecord.newBuilder()
        .setDatasetKey(metadata.getDataResourceUid())
        .setDatasetTitle(metadata.getDataResourceName());
  }

  private void mapDenormalisedEvent(EventJsonRecord.Builder builder) {

    if (denormalisedEvent.getParents() != null & !denormalisedEvent.getParents().isEmpty()) {

      List<String> eventTypes = new ArrayList<>();
      List<String> eventIDs = new ArrayList<>();
      List<String> samplingProtocols = new ArrayList<>();

      boolean hasCoordsInfo = builder.getDecimalLatitude() != null;
      boolean hasCountryInfo = builder.getCountryCode() != null;
      boolean hasStateInfo = builder.getStateProvince() != null;
      boolean hasYearInfo = builder.getYear() != null;
      boolean hasMonthInfo = builder.getMonth() != null;
      boolean hasLocationID = builder.getLocationID() != null;
      boolean hasSamplingProtocol =
          builder.getSamplingProtocol() != null && !builder.getSamplingProtocol().isEmpty();

      // extract location & temporal information from
      denormalisedEvent
          .getParents()
          .forEach(
              parent -> {
                if (!hasYearInfo && parent.getYear() != null) {
                  builder.setYear(parent.getYear());
                }

                if (!hasMonthInfo && parent.getMonth() != null) {
                  builder.setMonth(parent.getMonth());
                }

                if (!hasCountryInfo && parent.getCountryCode() != null) {
                  builder.setCountryCode(parent.getCountryCode());
                }

                if (!hasStateInfo && parent.getStateProvince() != null) {
                  builder.setStateProvince(parent.getStateProvince());
                }

                if (!hasCoordsInfo
                    && parent.getDecimalLatitude() != null
                    && parent.getDecimalLongitude() != null) {
                  builder
                      .setHasCoordinate(true)
                      .setDecimalLatitude(parent.getDecimalLatitude())
                      .setDecimalLongitude(parent.getDecimalLongitude())
                      // geo_point
                      .setCoordinates(
                          JsonConverter.convertCoordinates(
                              parent.getDecimalLongitude(), parent.getDecimalLatitude()))
                      // geo_shape
                      .setScoordinates(
                          JsonConverter.convertScoordinates(
                              parent.getDecimalLongitude(), parent.getDecimalLatitude()));
                }

                if (!hasLocationID && parent.getLocationID() != null) {
                  builder.setLocationID(parent.getLocationID());
                }

                if (!hasSamplingProtocol
                    && parent.getSamplingProtocol() != null
                    && !parent.getSamplingProtocol().isEmpty()) {
                  samplingProtocols.addAll(parent.getSamplingProtocol());
                }

                eventIDs.add(parent.getEventID());
                eventTypes.add(parent.getEventType());
              });

      if (!hasSamplingProtocol) {
        builder.setSamplingProtocol(
            samplingProtocols.stream()
                .filter(s -> StringUtils.isNotEmpty(s))
                .distinct()
                .collect(Collectors.toList()));
      }

      builder.setEventHierarchy(eventIDs);
      builder.setEventTypeHierarchy(eventTypes);
      builder.setEventHierarchyJoined(String.join(" / ", eventIDs));
      builder.setEventTypeHierarchyJoined(String.join(" / ", eventTypes));
      builder.setEventHierarchyLevels(eventIDs.size());
    } else {
      List<String> eventHierarchy = new ArrayList<>();
      if (builder.getParentEventID() != null) {
        eventHierarchy.add(builder.getParentEventID());
      }
      if (builder.getEventID() != null) {
        eventHierarchy.add(builder.getEventID());
      }
      builder.setEventHierarchy(eventHierarchy);
    }
  }

  private void mapEventCoreRecord(EventJsonRecord.Builder builder) {

    // Simple
    builder
        .setSampleSizeValue(eventCore.getSampleSizeValue())
        .setSampleSizeUnit(eventCore.getSampleSizeUnit())
        .setReferences(eventCore.getReferences())
        .setDatasetID(eventCore.getDatasetID())
        .setDatasetName(eventCore.getDatasetName())
        .setSamplingProtocol(eventCore.getSamplingProtocol())
        //        .setParentsLineage(convertParents(eventCore.getParentsLineage()))
        .setParentEventID(eventCore.getParentEventID())
        .setLocationID(eventCore.getLocationID());

    // Vocabulary
    JsonConverter.convertVocabularyConcept(eventCore.getEventType())
        .ifPresent(builder::setEventType);

    // License
    JsonConverter.convertLicense(eventCore.getLicense()).ifPresent(builder::setLicense);

    // Multivalue fields
    JsonConverter.convertToMultivalue(eventCore.getSamplingProtocol())
        .ifPresent(builder::setSamplingProtocolJoined);
  }

  private void mapTemporalRecord(EventJsonRecord.Builder builder) {

    builder
        .setYear(temporal.getYear())
        .setMonth(temporal.getMonth())
        .setDay(temporal.getDay())
        .setStartDayOfYear(temporal.getStartDayOfYear())
        .setEndDayOfYear(temporal.getEndDayOfYear())
        .setModified(temporal.getModified());

    JsonConverter.convertEventDate(temporal.getEventDate()).ifPresent(builder::setEventDate);
    JsonConverter.convertEventDateSingle(temporal).ifPresent(builder::setEventDateSingle);
  }

  private void mapLocationRecord(EventJsonRecord.Builder builder) {

    builder
        .setContinent(location.getContinent())
        .setWaterBody(location.getWaterBody())
        .setCountry(location.getCountry())
        .setCountryCode(location.getCountryCode())
        .setPublishingCountry(location.getPublishingCountry())
        .setStateProvince(location.getStateProvince())
        .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
        .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
        .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
        .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
        .setMaximumDistanceAboveSurfaceInMeters(location.getMaximumDistanceAboveSurfaceInMeters())
        .setMinimumDistanceAboveSurfaceInMeters(location.getMinimumDistanceAboveSurfaceInMeters())
        .setCoordinateUncertaintyInMeters(location.getCoordinateUncertaintyInMeters())
        .setCoordinatePrecision(location.getCoordinatePrecision())
        .setHasCoordinate(location.getHasCoordinate())
        .setRepatriated(location.getRepatriated())
        .setHasGeospatialIssue(location.getHasGeospatialIssue())
        .setLocality(location.getLocality())
        .setFootprintWKT(location.getFootprintWKT());

    // Coordinates
    Double decimalLongitude = location.getDecimalLongitude();
    Double decimalLatitude = location.getDecimalLatitude();
    if (decimalLongitude != null && decimalLatitude != null) {
      builder
          .setDecimalLatitude(decimalLatitude)
          .setDecimalLongitude(decimalLongitude)
          // geo_point
          .setCoordinates(JsonConverter.convertCoordinates(decimalLongitude, decimalLatitude))
          // geo_shape
          .setScoordinates(JsonConverter.convertScoordinates(decimalLongitude, decimalLatitude));
    }

    JsonConverter.convertGadm(location.getGadm()).ifPresent(builder::setGadm);
  }

  protected void mapTaxonRecord(EventJsonRecord.Builder builder) {}

  private void mapMultimediaRecord(EventJsonRecord.Builder builder) {
    builder
        .setMultimediaItems(JsonConverter.convertMultimediaList(multimedia))
        .setMediaTypes(JsonConverter.convertMultimediaType(multimedia))
        .setMediaLicenses(JsonConverter.convertMultimediaLicense(multimedia));
  }

  private void mapMeasurementOrFactRecord(EventJsonRecord.Builder builder) {
    builder.setMeasurementOrFactMethods(
        measurementOrFactRecord.getMeasurementOrFactItems().stream()
            .map(MeasurementOrFact::getMeasurementMethod)
            .filter(x -> StringUtils.isNotEmpty(x))
            .distinct()
            .collect(Collectors.toList()));
    builder.setMeasurementOrFactTypes(
        measurementOrFactRecord.getMeasurementOrFactItems().stream()
            .map(MeasurementOrFact::getMeasurementType)
            .filter(x -> StringUtils.isNotEmpty(x))
            .distinct()
            .collect(Collectors.toList()));

    List<MeasurementOrFactJsonRecord> mofs =
        measurementOrFactRecord.getMeasurementOrFactItems().stream()
            .map(
                mor -> {
                  return MeasurementOrFactJsonRecord.newBuilder()
                      .setMeasurementID(mor.getMeasurementID())
                      .setMeasurementMethod(mor.getMeasurementMethod())
                      .setMeasurementType(mor.getMeasurementType())
                      .setMeasurementValue(mor.getMeasurementValue())
                      .setMeasurementUnit(mor.getMeasurementUnit())
                      .build();
                })
            .collect(Collectors.toList());
    builder.setMeasurementOrFacts(mofs);
  }

  private void mapExtendedRecord(EventJsonRecord.Builder builder) {
    builder.setExtensions(JsonConverter.convertExtensions(verbatim));

    // set occurrence count
    Integer occurrenceCount =
        Optional.of(verbatim.getExtensions())
            .map(exts -> exts.get(DwcTerm.Occurrence.qualifiedName()))
            .map(ext -> ext.size())
            .orElse(0);

    builder.setOccurrenceCount(occurrenceCount);

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventID);
    extractOptValue(verbatim, DwcTerm.parentEventID).ifPresent(builder::setParentEventID);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.verbatimDepth).ifPresent(builder::setVerbatimDepth);
    extractOptValue(verbatim, DwcTerm.verbatimElevation).ifPresent(builder::setVerbatimElevation);
    extractOptValue(verbatim, DwcTerm.locationID).ifPresent(builder::setLocationID);

    String eventName = verbatim.getCoreTerms().get("http://rs.gbif.org/terms/1.0/eventName");
    if (eventName != null) {
      builder.setEventName(eventName);
    }
  }

  private void mapCreated(ParentJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(metadata, eventCore, temporal, location, multimedia)
        .ifPresent(builder::setCreated);
  }

  private void mapDerivedMetadata(ParentJsonRecord.Builder builder) {
    builder.setDerivedMetadata(derivedMetadata);
  }

  //  protected static List<Parent> convertParents(List<org.gbif.pipelines.io.avro.Parent> parents)
  // {
  //    if (parents == null) {
  //      return Collections.emptyList();
  //    }
  //
  //    return parents.stream()
  //        .map(p -> Parent.newBuilder().setId(p.getId()).setEventType(p.getEventType()).build())
  //        .collect(Collectors.toList());
  //  }
}
