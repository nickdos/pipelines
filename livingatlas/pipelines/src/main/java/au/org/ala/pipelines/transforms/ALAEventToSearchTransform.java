package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENTS_AVRO_TO_JSON_COUNT;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.gbif.pipelines.io.avro.*;

/**
 * Beam level transformation for the ES output json. The transformation consumes objects, which
 * classes were generated from avro schema files and converts into json string object
 *
 * <p>Example:
 *
 * <p>
 *
 * <pre>{@code
 * final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
 * final TupleTag<IdentifierRecord> irTag = new TupleTag<IdentifierRecord>() {};
 * final TupleTag<EventCoreRecord> ecrTag = new TupleTag<EventCoreRecord>() {};
 *
 * PCollection<KV<String, ExtendedRecord>> verbatimCollection = ...
 * PCollection<KV<String, EventCoreRecord>> eventCoreRecordCollection = ...
 * PCollection<KV<String, IdentifierRecord>> identifierRecordCollection = ...
 *
 * SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
 * EventCoreJsonTransform.builder()
 *    .extendedRecordTag(verbatimTransform.getTag())
 *    .identifierRecordTag(identifierTransform.getTag())
 *    .eventCoreRecordTag(eventCoreTransform.getTag())
 *    .build()
 *    .converter();
 *
 * PCollection<String> jsonCollection =
 *    KeyedPCollectionTuple
 *    // Core
 *    .of(eventCoreTransform.getTag(), eventCoreCollection)
 *    // Internal
 *    .and(identifierTransform.getTag(), identifierCollection)
 *    // Raw
 *    .and(verbatimTransform.getTag(), verbatimCollection)
 *    // Apply
 *    .apply("Grouping objects", CoGroupByKey.create())
 *    .apply("Merging to json", eventJsonDoFn);
 * }</pre>
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class ALAEventToSearchTransform implements Serializable {

  private static final long serialVersionUID = 1279313941024805871L;

  // Core
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  // Extension
  @NonNull private final PCollectionView<ALAMetadataRecord> metadataView;
  @NonNull private final TupleTag<MeasurementOrFactRecord> measurementOrFactRecordTag;
  private final TupleTag<DenormalisedEvent> denormalisedEventTag;
  private final TupleTag<String[]> samplingProtocolsTag;

  private final TupleTag<String[]> taxonIDsTag;

  public SingleOutput<KV<String, CoGbkResult>, EventSearchRecord> converter() {

    DoFn<KV<String, CoGbkResult>, EventSearchRecord> fn =
        new DoFn<KV<String, CoGbkResult>, EventSearchRecord>() {

          private final Counter counter =
              Metrics.counter(ALAEventToSearchTransform.class, EVENTS_AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            ALAMetadataRecord mdr = c.sideInput(metadataView);
            // Core
            EventCoreRecord core =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            // De-normed events
            DenormalisedEvent de =
                v.getOnly(denormalisedEventTag, DenormalisedEvent.newBuilder().setId(k).build());
            MeasurementOrFactRecord mofr =
                v.getOnly(
                    measurementOrFactRecordTag,
                    MeasurementOrFactRecord.newBuilder().setId(k).build());

            String[] sps = v.getOnly(samplingProtocolsTag, new String[0]);
            String[] taxonIDs = v.getOnly(taxonIDsTag, new String[0]);

            // Convert and
            List<DenormalisedParentEvent> parents = de.getParents();
            EventSearchRecord.Builder builder = EventSearchRecord.newBuilder().setId(core.getId());

            // set mof
            builder.setMeasurementOrFactTypes(
                mofr.getMeasurementOrFactItems().stream()
                    .map(MeasurementOrFact::getMeasurementType)
                    .filter(x -> StringUtils.isNotEmpty(x))
                    .distinct()
                    .collect(Collectors.toList()));
            builder
                .setDatasetKey(mdr.getDataResourceUid())
                .setTaxonKey(Arrays.asList(taxonIDs))
                .setLocationID(
                    consolidate(
                        core.getLocationID(),
                        parents.stream().map(x -> x.getLocationID()).collect(Collectors.toList())))
                .setYear(tr.getYear())
                .setMonth(tr.getMonth())
                .setCountryCode(
                    consolidate(
                        lr.getCountryCode(),
                        parents.stream().map(x -> x.getCountryCode()).collect(Collectors.toList())))
                .setStateProvince(
                    consolidate(
                        lr.getStateProvince(),
                        parents.stream()
                            .map(x -> x.getStateProvince())
                            .collect(Collectors.toList())))
                .setSamplingProtocol(
                    consolidate(
                        core.getSamplingProtocol(),
                        parents.stream()
                            .map(x -> x.getSamplingProtocol())
                            .collect(Collectors.toList())))
                .setEventTypeHierarchy(
                    consolidate(
                        core.getEventType() != null ? core.getEventType().getConcept() : "",
                        parents.stream().map(x -> x.getEventType()).collect(Collectors.toList())))
                .setEventHierarchy(
                    consolidate(
                        core.getId(),
                        parents.stream().map(x -> x.getEventID()).collect(Collectors.toList())));

            boolean hasYearInfo = builder.getYear() != null;
            boolean hasMonthInfo = builder.getMonth() != null;

            // extract location & temporal information from
            parents.forEach(
                parent -> {
                  if (!hasYearInfo && parent.getYear() != null) {
                    builder.setYear(parent.getYear());
                  }

                  if (!hasMonthInfo && parent.getMonth() != null) {
                    builder.setMonth(parent.getMonth());
                  }
                });

            c.output(builder.build());
          }
        };
    return ParDo.of(fn).withSideInputs(metadataView);
  }

  public List<String> consolidate(String value, List<String> denormed) {
    List<String> list = denormed.stream().distinct().collect(Collectors.toList());
    list.add(value);
    return list.stream()
        .distinct()
        .filter(x -> StringUtils.isNotEmpty(x))
        .collect(Collectors.toList());
  }

  public List<String> consolidate(List<String> value, List<List<String>> denormed) {
    List<String> list =
        denormed.stream().flatMap(List::stream).distinct().collect(Collectors.toList());
    list.addAll(value);
    return list.stream()
        .distinct()
        .filter(x -> StringUtils.isNotEmpty(x))
        .collect(Collectors.toList());
  }

  public List<Integer> consolidate(Integer value, List<Integer> denormed) {
    List<Integer> list = denormed.stream().distinct().collect(Collectors.toList());
    list.add(value);
    return list.stream().distinct().filter(x -> x != null).collect(Collectors.toList());
  }
}
