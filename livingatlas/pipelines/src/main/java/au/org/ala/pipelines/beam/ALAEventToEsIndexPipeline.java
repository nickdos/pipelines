package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.util.ElasticsearchTools;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link EventCoreRecord},
 *      {@link IdentifierRecord},
 *      {@link ExtendedRecord},
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-event-core-schema.json)
 *    4) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/examples-pipelines-BUILD_VERSION-shaded.jar
 *  --pipelineStep=INTERPRETED_TO_ES_INDEX \
 *  --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 *  --attempt=1 \
 *  --runner=SparkRunner \
 *  --inputPath=/path \
 *  --targetPath=/path \
 *  --esIndexName=test2_java \
 *  --esAlias=occurrence2_java \
 *  --indexNumberShards=3 \
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200 \
 * --esDocumentId=id
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAEventToEsIndexPipeline {

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "elastic");
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(combinedArgs);
    run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      EsIndexingPipelineOptions options,
      Function<EsIndexingPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    String esDocumentId = options.getEsDocumentId();

    ElasticsearchTools.createIndexAndAliasForDefault(options);

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Event, t, ALL_AVRO);

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, ALL_AVRO);

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    FileSystem fs =
        FileSystemFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()))
            .getFs(options.getInputPath());

    boolean datasetHasOccurrences =
        ValidationUtils.isInterpretationAvailable(
            fs, options.getInputPath(), options.getDatasetId(), options.getAttempt());

    options.setAppName("Event indexing of " + options.getDatasetId());
    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    ALAMetadataTransform metadataTransform = ALAMetadataTransform.builder().create();
    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    IdentifierTransform identifierTransform = IdentifierTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    LocationTransform parentLocationTransform = LocationTransform.builder().create();

    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    System.out.println("Adding step 3: Creating beam pipeline");
    PCollectionView<ALAMetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(pathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> identifierCollection =
        p.apply("Read identifiers", identifierTransform.read(pathFn))
            .apply("Map identifiers to KV", identifierTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", eventCoreTransform.read(pathFn))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(pathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, ALATaxonRecord>> taxonCollection =
        p.apply("Read event taxon records", alaTaxonomyTransform.read(pathFn))
            .apply("Map event taxon records to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", imageTransform.read(pathFn))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", audubonTransform.read(pathFn))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementOrFactCollection =
        p.apply("Read Measurement or fact", measurementOrFactTransform.read(pathFn))
            .apply("Map Measurement or fact to KV", measurementOrFactTransform.toKv());

    PCollection<KV<String, DenormalisedEvent>> denormCollection =
        getEventDenormalisation(options, p);

    PCollection<KV<String, String[]>> denormedSamplingProtocols =
        denormaliseSamplingProtocols(denormCollection, p);

    TupleTag<DenormalisedEvent> denormalisedEventTag = new TupleTag<>();
    TupleTag<String[]> samplingProtocolsTag = new TupleTag<>();

    PCollection<KV<String, DerivedMetadataRecord>> derivedMetadataRecordCollection =
        DerivedMetadata.builder()
            .pipeline(p)
            .verbatimTransform(verbatimTransform)
            .temporalTransform(temporalTransform)
            .parentLocationTransform(parentLocationTransform)
            .locationTransform(locationTransform)
            .eventCoreTransform(eventCoreTransform)
            .verbatimCollection(verbatimCollection)
            .temporalCollection(temporalCollection)
            .locationCollection(locationCollection)
            .taxonCollection(taxonCollection)
            .eventCoreCollection(eventCoreCollection)
            .occurrencesPathFn(occurrencesPathFn)
            .build()
            .calculate();

    System.out.println("Adding step 3: Converting into a json object");
    SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
        ALAParentJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(identifierTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .measurementOrFactRecordTag(measurementOrFactTransform.getTag())
            .denormalisedEventTag(denormalisedEventTag)
            .derivedMetadataRecordTag(DerivedMetadataTransform.tag())
            .samplingProtocolsTag(samplingProtocolsTag)
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<String> eventJsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            // Internal
            .and(identifierTransform.getTag(), identifierCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            .and(measurementOrFactTransform.getTag(), measurementOrFactCollection)
            // denorm
            .and(denormalisedEventTag, denormCollection)
            // derived metadata
            .and(DerivedMetadataTransform.tag(), derivedMetadataRecordCollection)
            .and(samplingProtocolsTag, denormedSamplingProtocols)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventJsonDoFn);

    String denormPath =
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId().trim(),
            options.getAttempt().toString(),
            "event",
            "event_hierarchy",
            "*.avro");

    log.info("Using denorm events path  " + denormPath);

    PCollection<String> occurrenceJsonCollection =
        datasetHasOccurrences
            ? ALAOccurrenceToEsIndexPipeline.IndexingTransform.builder()
                .pipeline(p)
                .identifiersPathFn(identifiersPathFn)
                .pathFn(occurrencesPathFn)
                .denormEventsPath(denormPath)
                .asParentChildRecord(true)
                .build()
                .apply()
            : p.apply("Create empty occurrenceJsonCollection", Create.empty(StringUtf8Coder.of()));

    // Merge events and occurrences
    PCollection<String> jsonCollection =
        PCollectionList.of(eventJsonCollection)
            .and(occurrenceJsonCollection)
            .apply("Join event and occurrence Json records", Flatten.pCollections());

    log.info("Adding step 6: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
                options.getEsHosts(), options.getEsIndexName(), "_doc")
            .withConnectTimeout(180000);

    if (Objects.nonNull(options.getEsUsername()) && Objects.nonNull(options.getEsPassword())) {
      esConfig =
          esConfig.withUsername(options.getEsUsername()).withPassword(options.getEsPassword());
    }

    ElasticsearchIO.Write writeIO =
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withRoutingFn(
                new ElasticsearchIO.Write.FieldValueExtractFn() {
                  @Override
                  public String apply(JsonNode input) {
                    return Optional.of(input.get("joinRecord"))
                        .filter(i -> i.hasNonNull("parent"))
                        .map(i -> i.get("parent").asText())
                        .orElse(input.get("internalId").asText());
                  }
                })
            .withMaxBatchSize(options.getEsMaxBatchSize());

    // Ignore gbifID as ES doc ID, useful for validator
    if (esDocumentId != null && !esDocumentId.isEmpty()) {
      writeIO =
          writeIO.withIdFn(
              new ElasticsearchIO.Write.FieldValueExtractFn() {
                @Override
                public String apply(JsonNode input) {
                  return input.get(esDocumentId).asText();
                }
              });
    }

    jsonCollection.apply(writeIO);

    // run the AVRO builder
    ALAEventToSearchAvroPipeline.run(options);

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
      EsIndexingPipelineOptions options, Pipeline p) {
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
