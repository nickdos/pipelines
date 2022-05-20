package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CLONING_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GEL_IMAGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFIER_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOAN_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MATERIAL_SAMPLE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_SCORE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PERMIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PREPARATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PRESERVATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.REFERENCE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.*;
import static org.gbif.pipelines.ingest.java.transforms.InterpretedAvroReader.readAvroAsFuture;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.converters.AmplificationTableConverter;
import org.gbif.pipelines.core.converters.ChronometricAgeTableConverter;
import org.gbif.pipelines.core.converters.CloningTableConverter;
import org.gbif.pipelines.core.converters.ExtendedMeasurementOrFactTableConverter;
import org.gbif.pipelines.core.converters.GelImageTableConverter;
import org.gbif.pipelines.core.converters.GermplasmAccessionTableConverter;
import org.gbif.pipelines.core.converters.GermplasmMeasurementScoreTableConverter;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTraitTableConverter;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTrialTableConverter;
import org.gbif.pipelines.core.converters.IdentificationTableConverter;
import org.gbif.pipelines.core.converters.IdentifierTableConverter;
import org.gbif.pipelines.core.converters.LoanTableConverter;
import org.gbif.pipelines.core.converters.MaterialSampleTableConverter;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.core.converters.PermitTableConverter;
import org.gbif.pipelines.core.converters.PreparationTableConverter;
import org.gbif.pipelines.core.converters.PreservationTableConverter;
import org.gbif.pipelines.core.converters.ReferenceTableConverter;
import org.gbif.pipelines.core.converters.ResourceRelationshipTableConverter;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.java.transforms.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.ingest.java.transforms.TableConverter;
import org.gbif.pipelines.ingest.java.transforms.TableRecordWriter;
import org.gbif.pipelines.ingest.utils.HdfsViewAvroUtils;
import org.gbif.pipelines.ingest.utils.SharedLockUtils;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.extension.dwc.ChronometricAgeTable;
import org.gbif.pipelines.io.avro.extension.dwc.IdentificationTable;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.gbif.pipelines.io.avro.extension.dwc.ResourceRelationshipTable;
import org.gbif.pipelines.io.avro.extension.gbif.IdentifierTable;
import org.gbif.pipelines.io.avro.extension.gbif.ReferenceTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmAccessionTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementScoreTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTraitTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTrialTable;
import org.gbif.pipelines.io.avro.extension.ggbn.AmplificationTable;
import org.gbif.pipelines.io.avro.extension.ggbn.CloningTable;
import org.gbif.pipelines.io.avro.extension.ggbn.GelImageTable;
import org.gbif.pipelines.io.avro.extension.ggbn.LoanTable;
import org.gbif.pipelines.io.avro.extension.ggbn.MaterialSampleTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PermitTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PreparationTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PreservationTable;
import org.gbif.pipelines.io.avro.extension.obis.ExtendedMeasurementOrFactTable;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.wrangler.lock.Mutex;
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
 *      {@link AudubonRecord},
 *      {@link MeasurementOrFactRecord},
 *      {@link TaxonRecord},
 *      {@link GrscicollRecord},
 *      {@link LocationRecord}
 *    2) Joins avro files
 *    3) Converts to a {@link OccurrenceHdfsRecord} based on the input files
 *    4) Moves the produced files to a directory where the latest version of HDFS records are kept
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToHdfsViewPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToHdfsViewPipeline \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
 * --inputPath=/path \
 * --targetPath=/path \
 * --properties=/path/pipelines.properties
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceToHdfsViewPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  public static void main(String[] args) {
    run(args);
  }

  public static void run(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options, executor);
  }

  @SneakyThrows
  public static void run(InterpretationPipelineOptions options, ExecutorService executor) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.HDFS_VIEW.name());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Set<String> types = options.getInterpretationTypes();

    Set<String> deleteTypes =
        RecordType.getAllTables().stream().map(RecordType::name).collect(Collectors.toSet());

    // Deletes the target path if it exists
    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, options.getInputPath(), datasetId, attempt, CORE_TERM, deleteTypes);

    Function<InterpretationType, String> pathFn =
        st -> {
          String id = datasetId + '_' + attempt + AVRO_EXTENSION;
          return PathBuilder.buildFilePathViewUsingInputPath(options, st.name().toLowerCase(), id);
        };

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToHdfsViewMetrics();

    log.info("Creating pipeline");

    // Reading all avro files in parallel
    CompletableFuture<Map<String, MetadataRecord>> metadataMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, MetadataTransform.builder().create());

    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, VerbatimTransform.create());

    CompletableFuture<Map<String, GbifIdRecord>> idMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, GbifIdTransform.builder().create());

    CompletableFuture<Map<String, ClusteringRecord>> clusteringMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, ClusteringTransform.builder().create());

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, BasicTransform.builder().create());

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, TemporalTransform.builder().create());

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, LocationTransform.builder().create());

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, TaxonomyTransform.builder().create());

    CompletableFuture<Map<String, GrscicollRecord>> grscicollMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, GrscicollTransform.builder().create());

    CompletableFuture<Map<String, MultimediaRecord>> multimediaMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, MultimediaTransform.builder().create());

    CompletableFuture<Map<String, ImageRecord>> imageMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, ImageTransform.builder().create());

    CompletableFuture<Map<String, AudubonRecord>> audubonMapFeature =
        readAvroAsFuture(options, CORE_TERM, executor, AudubonTransform.builder().create());

    Map<String, GbifIdRecord> idRecordMap = idMapFeature.get();

    // OccurrenceHdfsRecord
    Function<GbifIdRecord, Optional<OccurrenceHdfsRecord>> occurrenceHdfsRecordFn =
        OccurrenceHdfsRecordConverter.builder()
            .metrics(metrics)
            .metadata(metadataMapFeature.get().values().iterator().next())
            .verbatimMap(verbatimMapFeature.get())
            .clusteringMap(clusteringMapFeature.get())
            .basicMap(basicMapFeature.get())
            .temporalMap(temporalMapFeature.get())
            .locationMap(locationMapFeature.get())
            .taxonMap(taxonMapFeature.get())
            .grscicollMap(grscicollMapFeature.get())
            .multimediaMap(multimediaMapFeature.get())
            .imageMap(imageMapFeature.get())
            .audubonMap(audubonMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<OccurrenceHdfsRecord>builder()
        .recordFunction(occurrenceHdfsRecordFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(OccurrenceHdfsRecord.getClassSchema())
        .executor(executor)
        .options(options)
        .types(Collections.singleton(OCCURRENCE.name()))
        .recordType(OCCURRENCE)
        .build()
        .write();

    // MeasurementOrFactTable
    Function<GbifIdRecord, Optional<MeasurementOrFactTable>> measurementOrFactFn =
        TableConverter.<MeasurementOrFactTable>builder()
            .metrics(metrics)
            .converterFn(MeasurementOrFactTableConverter::convert)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MeasurementOrFactTable>builder()
        .recordFunction(measurementOrFactFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(MeasurementOrFactTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(MEASUREMENT_OR_FACT_TABLE)
        .types(types)
        .build()
        .write();

    // IdentificationTable
    Function<GbifIdRecord, Optional<IdentificationTable>> identificationFn =
        TableConverter.<IdentificationTable>builder()
            .metrics(metrics)
            .converterFn(IdentificationTableConverter::convert)
            .counterName(IDENTIFICATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<IdentificationTable>builder()
        .recordFunction(identificationFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(IdentificationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(IDENTIFICATION_TABLE)
        .types(types)
        .build()
        .write();

    // ResourceRelationTable
    Function<GbifIdRecord, Optional<ResourceRelationshipTable>> resourceRelationFn =
        TableConverter.<ResourceRelationshipTable>builder()
            .metrics(metrics)
            .converterFn(ResourceRelationshipTableConverter::convert)
            .counterName(RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ResourceRelationshipTable>builder()
        .recordFunction(resourceRelationFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(ResourceRelationshipTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(RESOURCE_RELATIONSHIP_TABLE)
        .types(types)
        .build()
        .write();

    // AmplificationTable
    Function<GbifIdRecord, Optional<AmplificationTable>> amplificationFn =
        TableConverter.<AmplificationTable>builder()
            .metrics(metrics)
            .converterFn(AmplificationTableConverter::convert)
            .counterName(AMPLIFICATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<AmplificationTable>builder()
        .recordFunction(amplificationFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(AmplificationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(AMPLIFICATION_TABLE)
        .types(types)
        .build()
        .write();

    // CloningTable
    Function<GbifIdRecord, Optional<CloningTable>> cloningFn =
        TableConverter.<CloningTable>builder()
            .metrics(metrics)
            .converterFn(CloningTableConverter::convert)
            .counterName(CLONING_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<CloningTable>builder()
        .recordFunction(cloningFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(CloningTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(CLONING_TABLE)
        .types(types)
        .build()
        .write();

    // GelImageTable
    Function<GbifIdRecord, Optional<GelImageTable>> gelImageFn =
        TableConverter.<GelImageTable>builder()
            .metrics(metrics)
            .converterFn(GelImageTableConverter::convert)
            .counterName(GEL_IMAGE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GelImageTable>builder()
        .recordFunction(gelImageFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(GelImageTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(GEL_IMAGE_TABLE)
        .types(types)
        .build()
        .write();

    // LoanTable
    Function<GbifIdRecord, Optional<LoanTable>> loanFn =
        TableConverter.<LoanTable>builder()
            .metrics(metrics)
            .converterFn(LoanTableConverter::convert)
            .counterName(LOAN_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<LoanTable>builder()
        .recordFunction(loanFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(LoanTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(LOAN_TABLE)
        .types(types)
        .build()
        .write();

    // MaterialSampleTable
    Function<GbifIdRecord, Optional<MaterialSampleTable>> materialSampleFn =
        TableConverter.<MaterialSampleTable>builder()
            .metrics(metrics)
            .converterFn(MaterialSampleTableConverter::convert)
            .counterName(MATERIAL_SAMPLE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MaterialSampleTable>builder()
        .recordFunction(materialSampleFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(MaterialSampleTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(MATERIAL_SAMPLE_TABLE)
        .types(types)
        .build()
        .write();

    // PermitTable
    Function<GbifIdRecord, Optional<PermitTable>> permitFn =
        TableConverter.<PermitTable>builder()
            .metrics(metrics)
            .converterFn(PermitTableConverter::convert)
            .counterName(PERMIT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<PermitTable>builder()
        .recordFunction(permitFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(PermitTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(PERMIT_TABLE)
        .types(types)
        .build()
        .write();

    // PreparationTable
    Function<GbifIdRecord, Optional<PreparationTable>> preparationFn =
        TableConverter.<PreparationTable>builder()
            .metrics(metrics)
            .converterFn(PreparationTableConverter::convert)
            .counterName(PREPARATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<PreparationTable>builder()
        .recordFunction(preparationFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(PreparationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(PREPARATION_TABLE)
        .types(types)
        .build()
        .write();

    // PreservationTable
    Function<GbifIdRecord, Optional<PreservationTable>> preservationFn =
        TableConverter.<PreservationTable>builder()
            .metrics(metrics)
            .converterFn(PreservationTableConverter::convert)
            .counterName(PRESERVATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<PreservationTable>builder()
        .recordFunction(preservationFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(PreservationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(PRESERVATION_TABLE)
        .types(types)
        .build()
        .write();

    // MeasurementScoreTable
    Function<GbifIdRecord, Optional<GermplasmMeasurementScoreTable>> measurementScoreFn =
        TableConverter.<GermplasmMeasurementScoreTable>builder()
            .metrics(metrics)
            .converterFn(GermplasmMeasurementScoreTableConverter::convert)
            .counterName(MEASUREMENT_SCORE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GermplasmMeasurementScoreTable>builder()
        .recordFunction(measurementScoreFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(GermplasmMeasurementScoreTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(GERMPLASM_MEASUREMENT_SCORE_TABLE)
        .types(types)
        .build()
        .write();

    // MeasurementTraitTable
    Function<GbifIdRecord, Optional<GermplasmMeasurementTraitTable>> measurementTraitFn =
        TableConverter.<GermplasmMeasurementTraitTable>builder()
            .metrics(metrics)
            .converterFn(GermplasmMeasurementTraitTableConverter::convert)
            .counterName(MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GermplasmMeasurementTraitTable>builder()
        .recordFunction(measurementTraitFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(GermplasmMeasurementTraitTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(GERMPLASM_MEASUREMENT_TRAIT_TABLE)
        .types(types)
        .build()
        .write();

    // MeasurementTrialTable
    Function<GbifIdRecord, Optional<GermplasmMeasurementTrialTable>> measurementTrialFn =
        TableConverter.<GermplasmMeasurementTrialTable>builder()
            .metrics(metrics)
            .converterFn(GermplasmMeasurementTrialTableConverter::convert)
            .counterName(MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GermplasmMeasurementTrialTable>builder()
        .recordFunction(measurementTrialFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(GermplasmMeasurementTrialTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(GERMPLASM_MEASUREMENT_TRIAL_TABLE)
        .types(types)
        .build()
        .write();

    // GermplasmAccessionTable
    Function<GbifIdRecord, Optional<GermplasmAccessionTable>> germplasmAccessionFn =
        TableConverter.<GermplasmAccessionTable>builder()
            .metrics(metrics)
            .converterFn(GermplasmAccessionTableConverter::convert)
            .counterName(GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GermplasmAccessionTable>builder()
        .recordFunction(germplasmAccessionFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(GermplasmAccessionTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(GERMPLASM_ACCESSION_TABLE)
        .types(types)
        .build()
        .write();

    // ExtendedMeasurementOrFactTable
    Function<GbifIdRecord, Optional<ExtendedMeasurementOrFactTable>> extendedMeasurementOrFactFn =
        TableConverter.<ExtendedMeasurementOrFactTable>builder()
            .metrics(metrics)
            .converterFn(ExtendedMeasurementOrFactTableConverter::convert)
            .counterName(EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ExtendedMeasurementOrFactTable>builder()
        .recordFunction(extendedMeasurementOrFactFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(ExtendedMeasurementOrFactTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(EXTENDED_MEASUREMENT_OR_FACT_TABLE)
        .types(types)
        .build()
        .write();

    // ChronometricAgeTable
    Function<GbifIdRecord, Optional<ChronometricAgeTable>> chronometricAgeFn =
        TableConverter.<ChronometricAgeTable>builder()
            .metrics(metrics)
            .converterFn(ChronometricAgeTableConverter::convert)
            .counterName(CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ChronometricAgeTable>builder()
        .recordFunction(chronometricAgeFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(ChronometricAgeTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(CHRONOMETRIC_AGE_TABLE)
        .types(types)
        .build()
        .write();

    // ReferencesTable
    Function<GbifIdRecord, Optional<ReferenceTable>> referencesFn =
        TableConverter.<ReferenceTable>builder()
            .metrics(metrics)
            .converterFn(ReferenceTableConverter::convert)
            .counterName(REFERENCE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ReferenceTable>builder()
        .recordFunction(referencesFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(ReferenceTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(REFERENCE_TABLE)
        .types(types)
        .build()
        .write();

    // IdentifierTable
    Function<GbifIdRecord, Optional<IdentifierTable>> identifierFn =
        TableConverter.<IdentifierTable>builder()
            .metrics(metrics)
            .converterFn(IdentifierTableConverter::convert)
            .counterName(IDENTIFIER_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<IdentifierTable>builder()
        .recordFunction(identifierFn)
        .gbifIdRecords(idRecordMap.values())
        .targetPathFn(pathFn)
        .schema(IdentifierTable.getClassSchema())
        .executor(executor)
        .options(options)
        .recordType(IDENTIFIER_TABLE)
        .types(types)
        .build()
        .write();

    // Move files
    Mutex.Action action = () -> HdfsViewAvroUtils.move(options);
    if (options.getTestMode()) {
      action.execute();
    } else {
      SharedLockUtils.doHdfsPrefixLock(options, action);
    }

    MetricsHandler.saveCountersToInputPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }
}