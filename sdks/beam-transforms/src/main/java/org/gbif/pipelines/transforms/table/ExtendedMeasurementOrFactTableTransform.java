package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ExtendedMeasurementOrFactTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.obis.ExtendedMeasurementOrFactTable;

public class ExtendedMeasurementOrFactTableTransform extends TableTransform {

  @Builder
  public ExtendedMeasurementOrFactTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types,
      String filesPrefix) {
    super(
        EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        ExtendedMeasurementOrFactTable.getClassSchema(),
        ExtendedMeasurementOrFactTableTransform.class.getName(),
        EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT,
        filesPrefix,
        ExtendedMeasurementOrFactTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
