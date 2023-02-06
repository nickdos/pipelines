package org.gbif.pipelines.transforms.table;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.pojo.ErIdrMdrContainer;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;

@SuppressWarnings("ConstantConditions")
public abstract class TableTransform extends DoFn<KV<String, CoGbkResult>, GenericRecord> {

  @NonNull private final InterpretationType recordType;

  @NonNull
  private final SerializableFunction<ErIdrMdrContainer, List<? extends GenericRecord>> convertFn;

  @NonNull private TupleTag<ExtendedRecord> extendedRecordTag;

  @NonNull private TupleTag<IdentifierRecord> identifierRecordTag;

  @NonNull private PCollectionView<MetadataRecord> metadataView;

  @NonNull private String path;

  @NonNull private Integer numShards;

  @NonNull private Set<String> types;

  @NonNull private String filesPrefix;

  private final Counter counter;

  public TableTransform(
      InterpretationType recordType,
      String counterNamespace,
      String counterName,
      String filesPrefix,
      SerializableFunction<ErIdrMdrContainer, List<? extends GenericRecord>> convertFn) {
    this.filesPrefix = filesPrefix;
    this.recordType = recordType;
    this.counter = Metrics.counter(counterNamespace, counterName);
    this.convertFn = convertFn;
  }

  public TableTransform setExtendedRecordTag(TupleTag<ExtendedRecord> extendedRecordTag) {
    this.extendedRecordTag = extendedRecordTag;
    return this;
  }

  public TableTransform setIdentifierRecordTag(TupleTag<IdentifierRecord> identifierRecordTag) {
    this.identifierRecordTag = identifierRecordTag;
    return this;
  }

  public TableTransform setMetadataRecord(PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
    return this;
  }

  public TableTransform setPath(String path) {
    this.path = path;
    return this;
  }

  public TableTransform setNumShards(Integer numShards) {
    this.numShards = numShards;
    return this;
  }

  public TableTransform setTypes(Set<String> types) {
    this.types = types;
    return this;
  }

  public Optional<PCollection<KV<String, CoGbkResult>>> check(
      PCollection<KV<String, CoGbkResult>> pCollection) {
    return CheckTransforms.checkRecordType(types, recordType)
        ? Optional.of(pCollection)
        : Optional.empty();
  }

  public void write(PCollection<KV<String, CoGbkResult>> pCollection, Schema schema) {
    if (CheckTransforms.checkRecordType(types, recordType)) {
      pCollection
          .apply("Convert to " + recordType.name(), this.convert())
          .setCoder(AvroCoder.of(schema))
          .apply("Write " + recordType.name(), this.write(schema));
    }
  }

  public FileIO.Write<Void, GenericRecord> write(Schema schema) {

    FileIO.Write<Void, GenericRecord> write =
        FileIO.<GenericRecord>write()
            .via(ParquetIO.sink(schema).withCompressionCodec(CompressionCodecName.SNAPPY))
            .to(path)
            .withPrefix(filesPrefix)
            .withSuffix(PipelinesVariables.Pipeline.PARQUET_EXTENSION);

    if (numShards == null || numShards <= 0) {
      return write;
    } else {
      int shards = -Math.floorDiv(-numShards, 2);
      return write.withNumShards(shards);
    }
  }

  public SingleOutput<KV<String, CoGbkResult>, GenericRecord> convert() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    CoGbkResult v = c.element().getValue();
    String k = c.element().getKey();

    ExtendedRecord er = v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
    MetadataRecord mdr = c.sideInput(metadataView);
    IdentifierRecord id =
        v.getOnly(identifierRecordTag, IdentifierRecord.newBuilder().setId(k).build());

    convertFn
        .apply(ErIdrMdrContainer.create(er, id, mdr))
        .forEach(
            r -> {
              c.output(r);
              counter.inc();
            });
  }
}
