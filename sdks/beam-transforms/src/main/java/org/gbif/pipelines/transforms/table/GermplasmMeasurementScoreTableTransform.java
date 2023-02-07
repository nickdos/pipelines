package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_SCORE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_SCORE_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.GermplasmMeasurementScoreTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementScoreTable;

public class GermplasmMeasurementScoreTableTransform extends TableTransform {

  @Builder
  public GermplasmMeasurementScoreTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types,
      String filesPrefix) {
    super(
        GERMPLASM_MEASUREMENT_SCORE_TABLE,
        GermplasmMeasurementScoreTable.getClassSchema(),
        GermplasmMeasurementScoreTableTransform.class.getName(),
        MEASUREMENT_SCORE_TABLE_RECORDS_COUNT,
        filesPrefix,
        GermplasmMeasurementScoreTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
