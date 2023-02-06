package org.gbif.pipelines.transforms.table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** Hive client utility to execute simple queries and handling table partitions. */
@Data
@Builder
@Slf4j
public class HiveClient {

  private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  /**
   * Hive JDBC connection URL, use jdbc:hive2://host:port/database. The usual default port is 10000.
   */
  private final String connectionString;

  /** Hive username, if security is enabled. */
  @Builder.Default private final String hiveUsername = "";

  /** Hive password, if security is enabled. */
  @Builder.Default private final String hivePassword = "";

  // load Hive JDBC Driver
  static {
    loadHiveJdbDriver();
  }

  /** Loads Hive driver org.apache.hive.jdbc.HiveDriver. */
  private static void loadHiveJdbDriver() {
    try {
      Class.forName(HIVE_DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Executes a SQL statement that produces no-results. */
  @SneakyThrows
  public int sql(String sql) {
    try (Connection con =
            DriverManager.getConnection(connectionString, hiveUsername, hivePassword);
        PreparedStatement stmt = con.prepareStatement(sql)) {
      log.info("Running Hive query {}", sql);
      return stmt.executeUpdate();
    }
  }

  /**
   * Add an external partition to an existing table.
   *
   * @throws SQLException if the statement fails.
   */
  private void addPartition(
      String tableName, String partitionName, String partitionValue, String newLocation) {
    sql(
        String.format(
            "ALTER TABLE %s ADD PARTITION (%s=\"%s\") LOCATION '%s'",
            tableName, partitionName, partitionValue, newLocation));
  }

  /** Modifies the location of an existing partition */
  private void alterPartitionLocation(
      String tableName, String partitionName, String partitionValue, String newLocation) {
    sql(
        String.format(
            "ALTER TABLE %s PARTITION (%s=\"%s\") SET LOCATION '%s'",
            tableName, partitionName, partitionValue, newLocation));
  }

  /** Tries to add a partition to table, if the partition exists, its location is modified. */
  public void addOrAlterPartition(
      String tableName, String partitionName, String partitionValue, String newLocation) {
    try {
      addPartition(tableName, partitionName, partitionValue, newLocation);
    } catch (Exception ex) {
      if (isPartitionAlreadyExistingError(ex)) {
        alterPartitionLocation(tableName, partitionName, partitionValue, newLocation);
      }
    }
  }

  /** Is the SQLException wrapping an AlreadyExistsException from Hive. */
  private static boolean isPartitionAlreadyExistingError(Exception ex) {
    return ex instanceof SQLException && ex.getMessage().contains("AlreadyExistsException");
  }
}
