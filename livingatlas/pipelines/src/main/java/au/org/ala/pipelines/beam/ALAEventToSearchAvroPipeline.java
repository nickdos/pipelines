package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.transforms.ALAEventToSearchTransform;
import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.slf4j.MDC;

/** */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAEventToSearchAvroPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "interpret");
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(combinedArgs);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Event, t, ALL_AVRO);

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    options.setAppName("Event indexing of " + options.getDatasetId());
    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    ALAMetadataTransform metadataTransform = ALAMetadataTransform.builder().create();
    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();

    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();

    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    System.out.println("Adding step 3: Creating beam pipeline");
    PCollectionView<ALAMetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", eventCoreTransform.read(pathFn))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(pathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementOrFactCollection =
        p.apply("Read Measurement or fact", measurementOrFactTransform.read(pathFn))
            .apply("Map Measurement or fact to KV", measurementOrFactTransform.toKv());

    PCollection<KV<String, DenormalisedEvent>> denormCollection =
        getEventDenormalisation(options, p);

    PCollection<KV<String, String[]>> denormedSamplingProtocols =
        denormaliseSamplingProtocols(denormCollection, p);

    TupleTag<DenormalisedEvent> denormalisedEventTag = new TupleTag<>();
    TupleTag<String[]> samplingProtocolsTag = new TupleTag<>();
    TupleTag<String[]> taxonIDsTag = new TupleTag<>();

    // load the taxonomy from the occurrence records
    PCollection<KV<String, ALATaxonRecord>> taxonCollection =
        p.apply("Read Event core", alaTaxonomyTransform.read(occurrencesPathFn))
            .apply("Map Event core to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, String[]>> taxonIDCollection = keyByParentID(taxonCollection, p);

    System.out.println("Adding step 3: Converting into a json object");
    SingleOutput<KV<String, CoGbkResult>, EventSearchRecord> eventSearchAvroFn =
        ALAEventToSearchTransform.builder()
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .measurementOrFactRecordTag(measurementOrFactTransform.getTag())
            .denormalisedEventTag(denormalisedEventTag)
            .taxonIDsTag(taxonIDsTag)
            .samplingProtocolsTag(samplingProtocolsTag)
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<EventSearchRecord> eventSearchCollection =
        KeyedPCollectionTuple
            // Core
            .of(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Extension
            // Internal
            .and(measurementOrFactTransform.getTag(), measurementOrFactCollection)
            // denorm
            .and(denormalisedEventTag, denormCollection)
            // derived metadata
            .and(samplingProtocolsTag, denormedSamplingProtocols)
            .and(taxonIDsTag, taxonIDCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventSearchAvroFn);

    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "search",
            "event",
            "search");

    eventSearchCollection.apply(
        AvroIO.write(EventSearchRecord.class)
            .to(avroPath)
            .withSuffix(".avro")
            .withCodec(BASE_CODEC));

    log.info("Running the pipeline");
    try {
      PipelineResult result = p.run();
      result.waitUntilFinish();
      log.info("Save metrics into the file and set files owner");
      MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    } catch (Exception e) {
      log.error("Exception thrown", e);
      e.printStackTrace();
    }

    log.info("Pipeline has been finished");
  }

  private static PCollection<KV<String, String[]>> keyByParentID(
      PCollection<KV<String, ALATaxonRecord>> taxonCollection, Pipeline p) {
    return taxonCollection
        .apply(
            ParDo.of(
                new DoFn<KV<String, ALATaxonRecord>, KV<String, String[]>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, ALATaxonRecord> source,
                      OutputReceiver<KV<String, String[]>> out,
                      ProcessContext c) {
                    ALATaxonRecord taxon = source.getValue();
                    List<String> taxonID = new ArrayList<>();
                    taxonID.add(taxon.getKingdomID());
                    taxonID.add(taxon.getPhylumID());
                    taxonID.add(taxon.getOrderID());
                    taxonID.add(taxon.getClassID());
                    taxonID.add(taxon.getFamilyID());
                    taxonID.add(taxon.getGenusID());
                    taxonID.add(taxon.getSpeciesID());
                    taxonID.stream().filter(x -> x != null);
                    out.output(
                        KV.of(
                            taxon.getParentId(),
                            taxonID.stream()
                                .filter(x -> x != null)
                                .collect(Collectors.toList())
                                .toArray(new String[0])));
                  }
                })
            // group by eventID, distinct
            )
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<KV<String, Iterable<String[]>>, KV<String, String[]>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Iterable<String[]>> in,
                      OutputReceiver<KV<String, String[]>> out,
                      ProcessContext c) {
                    Iterable<String[]> taxonIDs = in.getValue();
                    Set<String> taxonIDSet = new HashSet<String>();
                    taxonIDs.forEach(list -> taxonIDSet.addAll(Arrays.asList(list)));
                    out.output(KV.of(in.getKey(), taxonIDSet.toArray(new String[0])));
                  }
                }));
  }

  private static PCollection<KV<String, String[]>> denormaliseSamplingProtocols(
      PCollection<KV<String, DenormalisedEvent>> denormCollection, Pipeline p) {
    return denormCollection
        .apply(
            ParDo.of(
                new DoFn<KV<String, DenormalisedEvent>, KV<String, String[]>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, DenormalisedEvent> source,
                      OutputReceiver<KV<String, String[]>> out,
                      ProcessContext c) {

                    List<DenormalisedParentEvent> lineage = source.getValue().getParents();

                    // get the distinct list of sampling protocols
                    String[] samplingProtocols =
                        lineage.stream()
                            .map(e -> e.getSamplingProtocol())
                            .flatMap(List::stream)
                            .distinct()
                            .collect(Collectors.toList())
                            .toArray(new String[0]);

                    List<String> eventIDs =
                        lineage.stream()
                            .map(e -> e.getEventID())
                            .distinct()
                            .collect(Collectors.toList());

                    eventIDs.forEach(eventID -> out.output(KV.of(eventID, samplingProtocols)));
                  }
                })

            // group by eventID, distinct
            )
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<KV<String, Iterable<String[]>>, KV<String, String[]>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Iterable<String[]>> in,
                      OutputReceiver<KV<String, String[]>> out,
                      ProcessContext c) {
                    Iterable<String[]> lineage = in.getValue();
                    Set<String> protocols = new HashSet<String>();
                    lineage.forEach(strArray -> protocols.addAll(Arrays.asList(strArray)));
                    out.output(KV.of(in.getKey(), protocols.toArray(new String[0])));
                  }
                }));
  }

  /** Load image service records for a dataset. */
  public static PCollection<KV<String, DenormalisedEvent>> getEventDenormalisation(
      InterpretationPipelineOptions options, Pipeline p) {
    PCollection<KV<String, DenormalisedEvent>> denorm =
        p.apply(
                AvroIO.read(DenormalisedEvent.class)
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
                    .from(
                        String.join(
                            "/",
                            options.getTargetPath(),
                            options.getDatasetId().trim(),
                            options.getAttempt().toString(),
                            "event",
                            "event_hierarchy",
                            "*.avro")))
            .apply(
                MapElements.into(new TypeDescriptor<KV<String, DenormalisedEvent>>() {})
                    .via((DenormalisedEvent tr) -> KV.of(tr.getId(), tr)));
    return denorm;
  }
}
