package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.transforms.ALAMetadataTransform;
import au.org.ala.pipelines.transforms.ALAOccurrenceJsonTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALAUUIDTransform;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link MetadataRecord},
 *      {@link BasicRecord},
 *      {@link TemporalRecord},
 *      {@link MultimediaRecord},
 *      {@link ImageRecord},
 *      {@link TaxonRecord},
 *      {@link GrscicollRecord},
 *      {@link LocationRecord}
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-occurrence-schema.json)
 *    4) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
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
 * --properties=/home/nvolik/Projects/GBIF/gbif-configuration/cli/dev/config/pipelines.properties \
 * --esDocumentId=id
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAOccurrenceToEsIndexPipeline {

  public static void main(String[] args) throws Exception {
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

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    String esDocumentId = options.getEsDocumentId();

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, ALL_AVRO);

    Pipeline p = pipelinesFn.apply(options);

    PCollection<String> jsonCollection =
        IndexingTransform.builder()
            .pipeline(p)
            .pathFn(pathFn)
            .identifiersPathFn(identifiersPathFn)
            .asParentChildRecord(false)
            .build()
            .apply();

    log.info("Adding step 4: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), "_doc");

    ElasticsearchIO.Write writeIO =
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize());

    // Ignore gbifID as ES doc ID, useful for validator
    if (esDocumentId != null && !esDocumentId.isEmpty()) {
      writeIO = writeIO.withIdFn(input -> input.get("id").asText());
    }

    jsonCollection.apply(writeIO);

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    FsUtils.setOwner(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
        metadataPath,
        "crap",
        "supergroup");

    log.info("Pipeline has been finished");
  }

  @Builder
  static class IndexingTransform {

    private final Pipeline pipeline;
    private final UnaryOperator<String> pathFn;

    private final UnaryOperator<String> identifiersPathFn;
    private final boolean asParentChildRecord;

    // Init transforms
    private final ALAUUIDTransform uuidTransform = ALAUUIDTransform.create();
    private final BasicTransform basicTransform = BasicTransform.builder().create();
    private final ALAMetadataTransform metadataTransform = ALAMetadataTransform.builder().create();
    private final VerbatimTransform verbatimTransform = VerbatimTransform.create();
    private final TemporalTransform temporalTransform = TemporalTransform.builder().create();
    private final ALATaxonomyTransform taxonomyTransform = ALATaxonomyTransform.builder().create();
    private final LocationTransform locationTransform = LocationTransform.builder().create();
    private final MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();

    PCollection<String> apply() {

      PCollectionView<ALAMetadataRecord> metadataView =
          pipeline
              .apply("Read occurrence Metadata", metadataTransform.read(pathFn))
              .apply("Convert to occurrence view", View.asSingleton());

      PCollection<KV<String, ALAUUIDRecord>> uuidCollection =
          pipeline
              .apply("Read occurrence Verbatim", uuidTransform.read(identifiersPathFn))
              .apply("Map occurrence Verbatim to KV", uuidTransform.toKv());

      PCollection<KV<String, ExtendedRecord>> verbatimCollection =
          pipeline
              .apply("Read occurrence Verbatim", verbatimTransform.read(pathFn))
              .apply("Map occurrence Verbatim to KV", verbatimTransform.toKv());

      PCollection<KV<String, BasicRecord>> basicCollection =
          pipeline
              .apply("Read occurrence Basic", basicTransform.read(pathFn))
              .apply("Map occurrence Basic to KV", basicTransform.toKv());

      PCollection<KV<String, TemporalRecord>> temporalCollection =
          pipeline
              .apply("Read occurrence Temporal", temporalTransform.read(pathFn))
              .apply("Map occurrence Temporal to KV", temporalTransform.toKv());

      PCollection<KV<String, LocationRecord>> locationCollection =
          pipeline
              .apply("Read occurrence Location", locationTransform.read(pathFn))
              .apply("Map occurrence Location to KV", locationTransform.toKv());

      PCollection<KV<String, ALATaxonRecord>> taxonCollection =
          pipeline
              .apply("Read occurrence Taxon", taxonomyTransform.read(pathFn))
              .apply("Map occurrence Taxon to KV", taxonomyTransform.toKv());

      PCollection<KV<String, MultimediaRecord>> multimediaCollection =
          pipeline
              .apply("Read occurrence Multimedia", multimediaTransform.read(pathFn))
              .apply("Map occurrence Multimedia to KV", multimediaTransform.toKv());

      log.info("Adding step: Converting into a occurrence json object");
      SingleOutput<KV<String, CoGbkResult>, String> occurrenceJsonDoFn =
          ALAOccurrenceJsonTransform.builder()
              .uuidRecordTag(uuidTransform.getTag())
              .extendedRecordTag(verbatimTransform.getTag())
              .basicRecordTag(basicTransform.getTag())
              .temporalRecordTag(temporalTransform.getTag())
              .locationRecordTag(locationTransform.getTag())
              .taxonRecordTag(taxonomyTransform.getTag())
              .multimediaRecordTag(multimediaTransform.getTag())
              .metadataView(metadataView)
              .asParentChildRecord(asParentChildRecord)
              .build()
              .converter();

      return KeyedPCollectionTuple
          // Core
          .of(basicTransform.getTag(), basicCollection)
          .and(uuidTransform.getTag(), uuidCollection)
          .and(temporalTransform.getTag(), temporalCollection)
          .and(locationTransform.getTag(), locationCollection)
          .and(taxonomyTransform.getTag(), taxonCollection)
          // Extension
          .and(multimediaTransform.getTag(), multimediaCollection)
          // Raw
          .and(verbatimTransform.getTag(), verbatimCollection)
          // Apply
          .apply("Grouping occurrence objects", CoGroupByKey.create())
          .apply("Merging to occurrence json", occurrenceJsonDoFn);
    }
  }
}
