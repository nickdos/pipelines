package org.gbif.pipelines.transforms.table;

import com.google.common.reflect.Reflection;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.beam.options.DataWarehousePipelineOptions;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Data
public class PartitionedTableTransform {

  private static final String EXT_PACKAGE = "org.gbif.pipelines.io.avro.extension";

  private final String tableName;

  private final String tableDirectoryName;

  private final HiveClient hiveClient;

  private final HdfsConfigs hdfsConfigs;

  private final String targetDir;

  @Builder
  public PartitionedTableTransform(
      Schema schema, HiveClient hiveClient, HdfsConfigs hdfsConfigs, String targetDir) {
    tableName = tableNameFromClass(schema.getFullName());
    tableDirectoryName = getTableDirectoryName(schema.getFullName());
    this.hiveClient = hiveClient;
    this.hdfsConfigs = hdfsConfigs;
    this.targetDir = targetDir;
  }

  private static HiveClient getHiveClient(DataWarehousePipelineOptions options) {
    return HiveClient.builder()
        .connectionString(options.getDwConnectionString())
        .hiveUsername(options.getDwUsername())
        .hivePassword(options.getDwUsername())
        .build();
  }

  public static void addOrUpdatePartition(
      DataWarehousePipelineOptions options, Schema schema, String location) {
    PartitionedTableTransform.builder()
        .hiveClient(getHiveClient(options))
        .hdfsConfigs(HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()))
        .schema(schema)
        .targetDir(options.getDwExternalStorePath())
        .build()
        .addOrUpdatePartition(options.getDatasetId(), location);
  }

  private static String tableNameFromClass(String className) {
    String packageName = Reflection.getPackageName(className);
    String simpleClassName = className.substring(packageName.length() + 1).toLowerCase();
    String leafNamespace = packageName.replace(EXT_PACKAGE + '.', "").replace('.', '_');
    return leafNamespace + '_' + simpleClassName.replace("table", "");
  }

  private static String getTableDirectoryName(String className) {
    String packageName = Reflection.getPackageName(className);
    return className.substring(packageName.length() + 1).toLowerCase();
  }

  public void addOrUpdatePartition(String datasetKey, String location) {
    hiveClient.addOrAlterPartition(tableName, "datasetkey", datasetKey, location);
    deletePreviousDatasetPartition(datasetKey, location);
  }

  @SneakyThrows
  private void deletePreviousDatasetPartition(String datasetKey, String location) {
    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getHdfsFs();
    FsUtils.deleteAllExcept(
        fs, new Path(targetDir, tableDirectoryName, datasetKey + "_*"), new Path(location));
  }
}
