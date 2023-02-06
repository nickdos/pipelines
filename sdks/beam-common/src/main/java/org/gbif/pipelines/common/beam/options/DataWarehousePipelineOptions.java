package org.gbif.pipelines.common.beam.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface DataWarehousePipelineOptions extends InterpretationPipelineOptions {

  @Description("JDBC Connection String to Hive or the underlying data warehouse")
  String getDwConnectionString();

  void setDwConnectionString(String dwConnectionString);

  @Description("Data warehouse username")
  @Default.String("")
  String getDwUsername();

  void setDwUsername(String dwUsername);

  @Description("Data warehouse password")
  @Default.String("")
  String getDwPassword();

  void setDwPassword(String dwPassword);

  @Description("Directory from where the Data warehouse reads the data")
  String getDwExternalStorePath();

  void setDwExternalStorePath(String DdwExternalStorePath);
}
