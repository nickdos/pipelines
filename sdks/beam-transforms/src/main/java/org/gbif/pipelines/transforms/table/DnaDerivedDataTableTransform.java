package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DNA_DERIVED_DATA_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.DNA_DERIVED_DATA_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.DnaDerivedDataTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

public class DnaDerivedDataTableTransform extends TableTransform {

  @Builder
  public DnaDerivedDataTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types,
      String filesPrefix) {
    super(
        DNA_DERIVED_DATA_TABLE,
        DnaDerivedDataTableTransform.class.getName(),
        DNA_DERIVED_DATA_TABLE_RECORDS_COUNT,
        filesPrefix,
        DnaDerivedDataTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
