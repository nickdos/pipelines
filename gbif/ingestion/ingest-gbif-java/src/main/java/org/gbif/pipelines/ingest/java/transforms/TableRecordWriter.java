package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.common.beam.options.DataWarehousePipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;

@Builder
public class TableRecordWriter<T extends GenericRecord> {

  @NonNull private final DataWarehousePipelineOptions options;
  @NonNull private final Collection<IdentifierRecord> identifierRecords;
  @NonNull private final Function<IdentifierRecord, List<T>> recordFunction;
  @NonNull private final Function<InterpretationType, String> targetPathFn;
  @NonNull private final Schema schema;
  @NonNull private final ExecutorService executor;
  @NonNull private final Set<String> types;
  @NonNull private final InterpretationType recordType;

  @SneakyThrows
  public void write() {
    if (CheckTransforms.checkRecordType(types, recordType)) {
      try (ParquetWriter<T> writer = createWriter(options)) {
        boolean useSyncMode = options.getSyncThreshold() > identifierRecords.size();
        if (useSyncMode) {
          syncWrite(writer);
        } else {
          CompletableFuture<?>[] futures = asyncWrite(writer);
          CompletableFuture.allOf(futures).get();
        }
      }
    }
  }

  /** Used to avoid verbose error handling in lambda calls. */
  @SneakyThrows
  private void writeRecord(ParquetWriter<T> writer, T record) {
    writer.write(record);
  }

  private CompletableFuture<?>[] asyncWrite(ParquetWriter<T> writer) {
    return identifierRecords.stream()
        .map(recordFunction)
        .flatMap(List::stream)
        .map(r -> CompletableFuture.runAsync(() -> writeRecord(writer, r), executor))
        .toArray(CompletableFuture[]::new);
  }

  private void syncWrite(ParquetWriter<T> writer) {
    identifierRecords.stream()
        .map(recordFunction)
        .flatMap(List::stream)
        .forEach(record -> writeRecord(writer, record));
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private ParquetWriter<T> createWriter(InterpretationPipelineOptions options) {
    Path path = new Path(targetPathFn.apply(recordType));

    Configuration hadoopConfiguration =
        FileSystemFactory.getHdfsConfiguration(options.getHdfsSiteConfig());

    createParentDirectories(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);

    return AvroParquetWriter.<T>builder(path)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withSchema(schema)
        .withConf(hadoopConfiguration)
        .build();
  }
}
