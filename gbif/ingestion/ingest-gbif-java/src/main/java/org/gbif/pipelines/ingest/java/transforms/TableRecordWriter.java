package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.common.beam.options.DataWarehousePipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;

@Builder
@Slf4j
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
        identifierRecords.stream()
            .map(recordFunction)
            .flatMap(List::stream)
            .forEach(r -> writeRecord(writer, r));
      } catch (Exception ex) {
        throw new PipelinesException(ex);
      }
    }
  }

  /** Used to avoid verbose error handling in lambda calls. */
  @SneakyThrows
  private void writeRecord(ParquetWriter<T> writer, T r) {
    try {
      writer.write(r);
    } catch (Exception ex) {
      log.error("Error writing record {}", r, ex);
      throw ex;
    }
  }

  /**
   * Create an AVRO file writer. Instances of it are not thread-safe and shouldn't be re-used in
   * treads.
   */
  @SneakyThrows
  private ParquetWriter<T> createWriter(InterpretationPipelineOptions options) {
    Path path = new Path(targetPathFn.apply(recordType));

    Configuration hadoopConfiguration =
        FileSystemFactory.getHdfsConfiguration(options.getHdfsSiteConfig());

    createParentDirectories(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);

    return AvroParquetWriter.<T>builder(HadoopOutputFile.fromPath(path, hadoopConfiguration))
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withSchema(schema)
        .withDataModel(GenericData.get())
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .config("parquet.avro.write-old-list-structure", "false")
        .build();
  }
}
