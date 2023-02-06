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

    copy(opt, coreType, MEASUREMENT_OR_FACT_TABLE, Extension.MEASUREMENT_OR_FACT);
    copy(opt, coreType, IDENTIFICATION_TABLE, Extension.IDENTIFICATION);
    copy(opt, coreType, RESOURCE_RELATIONSHIP_TABLE, Extension.RESOURCE_RELATIONSHIP);
    copy(opt, coreType, AMPLIFICATION_TABLE, Extension.AMPLIFICATION);
    copy(opt, coreType, CLONING_TABLE, Extension.CLONING);
    copy(opt, coreType, GEL_IMAGE_TABLE, Extension.GEL_IMAGE);
    copy(opt, coreType, LOAN_TABLE, Extension.LOAN);
    copy(opt, coreType, MATERIAL_SAMPLE_TABLE, Extension.MATERIAL_SAMPLE);
    copy(opt, coreType, PERMIT_TABLE, Extension.PERMIT);
    copy(opt, coreType, PREPARATION_TABLE, Extension.PREPARATION);
    copy(opt, coreType, PRESERVATION_TABLE, Extension.PRESERVATION);
    copy(opt, coreType, GERMPLASM_MEASUREMENT_SCORE_TABLE, Extension.GERMPLASM_MEASUREMENT_SCORE);
    copy(opt, coreType, GERMPLASM_MEASUREMENT_TRAIT_TABLE, Extension.GERMPLASM_MEASUREMENT_TRAIT);
    copy(opt, coreType, GERMPLASM_MEASUREMENT_TRIAL_TABLE, Extension.GERMPLASM_MEASUREMENT_TRIAL);
    copy(opt, coreType, GERMPLASM_ACCESSION_TABLE, Extension.GERMPLASM_ACCESSION);
    copy(opt, coreType, EXTENDED_MEASUREMENT_OR_FACT_TABLE, Extension.EXTENDED_MEASUREMENT_OR_FACT);
    copy(opt, coreType, CHRONOMETRIC_AGE_TABLE, Extension.CHRONOMETRIC_AGE);
    copy(opt, coreType, REFERENCE_TABLE, Extension.REFERENCE);
    copy(opt, coreType, IDENTIFIER_TABLE, Extension.IDENTIFIER);
    copy(opt, coreType, AUDUBON_TABLE, Extension.AUDUBON);
    copy(opt, coreType, IMAGE_TABLE, Extension.IMAGE);
    copy(opt, coreType, MULTIMEDIA_TABLE, Extension.MULTIMEDIA);
    copy(opt, coreType, DNA_DERIVED_DATA_TABLE, Extension.DNA_DERIVED_DATA);
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

  private static <T> void copy(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType,
      Extension extension) {
    String from = extensionRecordType.name().toLowerCase();
    String to = extension.name().toLowerCase().replace("_", "") + "table";
    copy(options, recordType, from, to);
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
