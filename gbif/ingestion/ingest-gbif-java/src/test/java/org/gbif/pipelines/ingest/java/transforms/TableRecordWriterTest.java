package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.PARQUET_EXTENSION;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.common.beam.options.DataWarehousePipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.junit.Assert;
import org.junit.Test;

public class TableRecordWriterTest {
  private static final String GBIF_ID = "777";

  @Test
  public void writerTest() throws Exception {

    // State
    IdentifierRecord idRecord =
        IdentifierRecord.newBuilder().setId("1").setInternalId(GBIF_ID).build();
    IdentifierRecord skipIdRecord =
        IdentifierRecord.newBuilder().setId("1").setInternalId("-" + GBIF_ID).build();
    List<IdentifierRecord> list = Arrays.asList(idRecord, skipIdRecord);

    Function<IdentifierRecord, List<OccurrenceHdfsRecord>> fn =
        id -> {
          if (id.getInternalId().startsWith("-")) {
            return Collections.emptyList();
          }
          OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();
          hdfsRecord.setGbifid(id.getInternalId());
          return Collections.singletonList(hdfsRecord);
        };

    String outputFile = getClass().getResource("/hdfsview/occurrence/").getFile();

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile,
      "--interpretationTypes=OCCURRENCE",
      "--dwConnectionString=jdbc:hive2://localhost:1000/default",
      "--dwExternalStorePath=/data/hive/"
    };
    DataWarehousePipelineOptions options =
        PipelinesOptionsFactory.createDataWarehousePipelineInterpretation(args);

    Function<InterpretationType, String> pathFn =
        st -> {
          String id = options.getDatasetId() + '_' + options.getAttempt() + PARQUET_EXTENSION;
          return PathBuilder.buildFilePathViewUsingInputPath(
              options,
              PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE,
              st.name().toLowerCase(),
              id);
        };

    // When
    TableRecordWriter.<OccurrenceHdfsRecord>builder()
        .recordFunction(fn)
        .identifierRecords(list)
        .executor(Executors.newSingleThreadExecutor())
        .options(options)
        .targetPathFn(pathFn)
        .schema(OccurrenceHdfsRecord.getClassSchema())
        .recordType(OCCURRENCE)
        .types(options.getInterpretationTypes())
        .build()
        .write();

    // Deserialize OccurrenceHdfsRecord from disk
    File result =
        new File(
            outputFile
                + "/d596fccb-2319-42eb-b13b-986c932780ad/146/occurrence_table/occurrence/d596fccb-2319-42eb-b13b-986c932780ad_146.parquet");

    Assert.assertTrue("File doesn't exist", result.exists());
    assertFile(result);

    Files.deleteIfExists(result.toPath());
  }

  private <T extends GenericRecord> void assertFile(File result) throws Exception {
    try (ParquetReader<T> dataFileReader =
        AvroParquetReader.<T>builder(
                HadoopInputFile.fromPath(new Path(result.toString()), new Configuration()))
            .build()) {
      T record;
      while (null != (record = dataFileReader.read())) {
        Assert.assertNotNull(record);
        Assert.assertEquals(GBIF_ID, record.get("gbifid"));
        System.out.println(record.get("gbifid"));
      }
    }
  }
}
