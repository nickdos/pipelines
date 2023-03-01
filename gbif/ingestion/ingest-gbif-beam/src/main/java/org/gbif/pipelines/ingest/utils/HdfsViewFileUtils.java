package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.*;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewFileUtils {

  /**
   * Cleans empty avro files for extensions and copies all avro files into the directory from
   * targetPath. Deletes pre-existing data of the dataset being processed.
   */
  public static void copy(InterpretationPipelineOptions options) {
    if (options.getInterpretationTypes().size() == 1
        && options.getInterpretationTypes().contains(OCCURRENCE.name())) {
      copyOccurrence(options);
    } else if (options.getInterpretationTypes().size() == 1
        && options.getInterpretationTypes().contains(EVENT.name())) {
      copy(options, EVENT);
    } else {
      moveAll(options);
    }
  }

  private static void copyOccurrence(InterpretationPipelineOptions options) {
    copy(options, OCCURRENCE);
  }

  private static void copyTables(InterpretationPipelineOptions opt, RecordType coreType) {
    cleanOrCopy(opt, coreType, MEASUREMENT_OR_FACT_TABLE, Extension.MEASUREMENT_OR_FACT);
    cleanOrCopy(opt, coreType, IDENTIFICATION_TABLE, Extension.IDENTIFICATION);
    cleanOrCopy(opt, coreType, RESOURCE_RELATIONSHIP_TABLE, Extension.RESOURCE_RELATIONSHIP);
    cleanOrCopy(opt, coreType, AMPLIFICATION_TABLE, Extension.AMPLIFICATION);
    cleanOrCopy(opt, coreType, CLONING_TABLE, Extension.CLONING);
    cleanOrCopy(opt, coreType, GEL_IMAGE_TABLE, Extension.GEL_IMAGE);
    cleanOrCopy(opt, coreType, LOAN_TABLE, Extension.LOAN);
    cleanOrCopy(opt, coreType, MATERIAL_SAMPLE_TABLE, Extension.MATERIAL_SAMPLE);
    cleanOrCopy(opt, coreType, PERMIT_TABLE, Extension.PERMIT);
    cleanOrCopy(opt, coreType, PREPARATION_TABLE, Extension.PREPARATION);
    cleanOrCopy(opt, coreType, PRESERVATION_TABLE, Extension.PRESERVATION);
    cleanOrCopy(
        opt, coreType, GERMPLASM_MEASUREMENT_SCORE_TABLE, Extension.GERMPLASM_MEASUREMENT_SCORE);
    cleanOrCopy(
        opt, coreType, GERMPLASM_MEASUREMENT_TRAIT_TABLE, Extension.GERMPLASM_MEASUREMENT_TRAIT);
    cleanOrCopy(
        opt, coreType, GERMPLASM_MEASUREMENT_TRIAL_TABLE, Extension.GERMPLASM_MEASUREMENT_TRIAL);
    cleanOrCopy(opt, coreType, GERMPLASM_ACCESSION_TABLE, Extension.GERMPLASM_ACCESSION);
    cleanOrCopy(
        opt, coreType, EXTENDED_MEASUREMENT_OR_FACT_TABLE, Extension.EXTENDED_MEASUREMENT_OR_FACT);
    cleanOrCopy(opt, coreType, CHRONOMETRIC_AGE_TABLE, Extension.CHRONOMETRIC_AGE);
    cleanOrCopy(opt, coreType, REFERENCE_TABLE, Extension.REFERENCE);
    cleanOrCopy(opt, coreType, IDENTIFIER_TABLE, Extension.IDENTIFIER);
    cleanOrCopy(opt, coreType, AUDUBON_TABLE, Extension.AUDUBON);
    cleanOrCopy(opt, coreType, IMAGE_TABLE, Extension.IMAGE);
    cleanOrCopy(opt, coreType, MULTIMEDIA_TABLE, Extension.MULTIMEDIA);
    cleanOrCopy(opt, coreType, DNA_DERIVED_DATA_TABLE, Extension.DNA_DERIVED_DATA);
  }

  private static void moveAll(InterpretationPipelineOptions options) {
    if (options.getCoreRecordType() == OCCURRENCE) {
      copy(options, OCCURRENCE);
      copyTables(options, OCCURRENCE);
    } else if (options.getCoreRecordType() == EVENT) {
      copy(options, EVENT);
      copyTables(options, EVENT);
    }
  }

  private static void copy(InterpretationPipelineOptions options, RecordType recordType) {
    String path = recordType.name().toLowerCase();
    copy(options, recordType, path, path);
  }

  /**
   * When we run type ALL, the process will produce empty files, so we have to check and delete them
   * in that case
   */
  private static void cleanOrCopy(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType,
      Extension extension) {
    String from = extensionRecordType.name().toLowerCase();
    String to = extension.name().toLowerCase().replace("_", "") + "table";
    boolean isCleaned = clean(options, recordType, extensionRecordType);
    if (!isCleaned) {
      copy(options, recordType, from, to);
    }
  }

  /** Delete file if it was created but has no records inside */
  private static boolean clean(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType) {
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    String extType = extensionRecordType.name().toLowerCase();
    String path = PathBuilder.buildFilePathViewUsingInputPath(options, recordType, extType);
    return FsUtils.deleteParquetFileIfEmpty(hdfsConfigs, path);
  }

  private static void copy(
      InterpretationPipelineOptions options, RecordType recordType, String from, String to) {
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    String deletePath =
        PathBuilder.buildPath(
                targetPath, recordType.name().toLowerCase(), to, options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting existing avro files {}", deletePath);
    FsUtils.deleteByPattern(hdfsConfigs, targetPath, deletePath);

    String filter =
        PathBuilder.buildFilePathViewUsingInputPath(options, recordType, from, "*.parquet");

    String movePath =
        PathBuilder.buildPath(targetPath, recordType.name().toLowerCase(), to).toString();
    log.info("Copying files with pattern {} to {}", filter, movePath);
    FsUtils.copyDirectory(hdfsConfigs, movePath, filter);
    log.info("Files copied to {} directory", movePath);
  }
}
