package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTraitTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTraitTable;

public class GermplasmMeasurementTraitTableTransform extends TableTransform {

  @Builder
  public GermplasmMeasurementTraitTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types,
      String filesPrefix) {
    super(
        GERMPLASM_MEASUREMENT_TRAIT_TABLE,
        GermplasmMeasurementTraitTable.getClassSchema(),
        GermplasmMeasurementTraitTableTransform.class.getName(),
        MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT,
        filesPrefix,
        GermplasmMeasurementTraitTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
