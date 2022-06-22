package org.gbif.pipelines.events
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.util
import java.util.UUID
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.xml.Elem

/**
 * Pipeline uses Spark SQL to produce a DwCA Archive.
 */
object DownloadDwCAPipeline {

  val workingDirectory = "/tmp/pipelines-export/"

  val FIELDS = Array(
    "core.id",
    "biome",
    "continent",
    "coordinatePrecision",
    "coordinateUncertaintyInMeters",
    "country",
    "countryCode",
//    "datasetID",
//    "datasetName",
    "dateIdentified",
    "datePrecision",
    "day",
    "decimalLatitude",
    "decimalLongitude",
    "depth",
    "depthAccuracy",
    "elevation",
    "elevationAccuracy",
    "endDayOfYear",
    "eventDate.gte",
    "eventType.concept",
    "footprintWKT",
    "georeferencedDate",
    "license",
    "locality",
    "locationID",
    "maximumDepthInMeters",
    "maximumDistanceAboveSurfaceInMeters",
    "maximumElevationInMeters",
    "minimumDepthInMeters",
    "minimumDistanceAboveSurfaceInMeters",
    "minimumElevationInMeters",
    "month",
    "parentEventID",
    "references",
    "sampleSizeUnit",
    "sampleSizeValue",
    "startDayOfYear",
    "stateProvince",
    "waterBody",
    "year"
  )

  // Test with some sample data
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Please supply <DatasetId> <HDFS_BasePath> <Attempt> <JobID>")
      println("e.g. dr18391 hdfs://localhost:9000/pipelines-data 1 job-123")
      return;
    }

    val datasetId = args(0)
    val hdfsPath = args(1)
    val attempt = args(2)
    val jobID = if (args.length > 3 && args(3) != null && args(3) != "generate") {
      args(3)
    } else {
      UUID.randomUUID.toString
    }

    val localTest = if (args.length == 5) {
      args(4).toBoolean
    } else {
      false
    }

    // Mask log
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = if (localTest) {
      SparkSession
        .builder
        .master("local[*]")
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
    } else {
      SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
    }

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    System.out.println("Load events")
    val eventCoreDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/event_core/*.avro").as("core")

    System.out.println("Load location")
    val locationDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/location/*.avro").as("location")

    System.out.println("Load temporal")
    val temporalDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/temporal/*.avro").as("temporal")

    System.out.println("Load verbatim")
    val verbatimDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/verbatim.avro").as("verbatim")

    val coreTermType = verbatimDF.select(col("coreRowType")).distinct.head.get(0).asInstanceOf[String]
    val coreTermTypeSimple = coreTermType.substring(coreTermType.lastIndexOf("/") + 1)

    System.out.println("Join")
    val filterDownloadDF = eventCoreDF.
      join(locationDF, col("core.id") === col("location.id"), "inner").
      join(temporalDF, col("core.id") === col("temporal.id"), "inner").
      join(verbatimDF, col("core.id") === col("verbatim.id"), "inner")

    // get a list columns
    val exportPath = workingDirectory + jobID + s"/${coreTermTypeSimple}/"


//    import org.apache.spark.sql.functions._
//
//    val stringify = udf((vs: Seq[String]) => vs match {
//      case null => null
//      case _    => s"""[${vs.mkString(",")}]"""
//    })
//
//    filterDownloadDF.withColumn("ArrayOfString", stringify($"ArrayOfString")).write.csv(...)


    // filter "coreTerms", "extensions"
    filterDownloadDF.select(generateFieldColumns(FIELDS):_*).coalesce(1).write
      .option("header","true")
      .option("sep","\t")
      .mode("overwrite")
      .csv(exportPath)

    cleanupFileExport(jobID, coreTermTypeSimple)

    val extensionsForMeta = new util.HashMap[String, Array[String]]

    // get list of extensions for this dataset
    val extensionList = getExtensionList(filterDownloadDF, spark)

    extensionList.foreach(extensionURI => {

      val extensionFields = getExtensionFields(filterDownloadDF, extensionURI, spark)

      val arrayStructureSchema = {
        var builder = new StructType().add("id", StringType)
        extensionFields.foreach { fieldName =>
          val isURI = fieldName.lastIndexOf("/") > 0
          val simpleName = if (isURI){
            fieldName.substring(fieldName.lastIndexOf("/") + 1)
          } else {
            fieldName
          }
          builder = builder.add(simpleName, StringType)
        }
        builder
      }

      val extensionDF = filterDownloadDF.select(
        col("core.id").as("id"),
        col(s"""extensions.`${extensionURI}`""").as("the_extension")
      ).toDF
      val rowRDD = extensionDF.rdd.map(row => genericRecordToRow(row, extensionFields, arrayStructureSchema)).flatMap(list => list)
      val extensionForExportDF = spark.sqlContext.createDataFrame(rowRDD, arrayStructureSchema)

      // filter "coreTerms", "extensions"
      val extensionSimpleName = extensionURI.substring(extensionURI.lastIndexOf("/") + 1)

      extensionForExportDF.select("*")
        .coalesce(1).write
        .option("header","true")
        .option("sep","\t")
        .mode("overwrite")
        .csv(workingDirectory + jobID + "/" + extensionSimpleName)

      cleanupFileExport(jobID, extensionSimpleName)
      extensionsForMeta.put(extensionURI, extensionFields)
    })

    // write the XML
    import scala.collection.JavaConversions._
    val metaXml = createMeta(coreTermType, FIELDS, extensionsForMeta.toMap)
    scala.xml.XML.save(workingDirectory + jobID + "/meta.xml", metaXml)

    // get EML doc
    import sys.process._
    new URL(s"https://collections-test.ala.org.au/ws/eml/${datasetId}") #> new File(workingDirectory + jobID + "/eml.xml") !!

    // create a zip
    val zip = new ZipOutputStream(new FileOutputStream(new File(workingDirectory + jobID + "/" + datasetId + ".zip")))
    new File(workingDirectory + jobID + "/").listFiles().foreach { file =>

      if (!file.getName.endsWith(datasetId + ".zip")) {
        println("Zipping " + file.getName)
        zip.putNextEntry(new ZipEntry(file.getName))
        val in = new BufferedInputStream(new FileInputStream(file))
        Stream.continually(in.read()).takeWhile(_ > -1).foreach { b => zip.write(b) }
        in.close()
        zip.flush()
        zip.closeEntry()
      }
    }
    zip.flush()
    zip.close()

    println("Export complete")
  }

  def generateFieldColumns(fields:Seq[String]): Seq[Column] = {
    fields.map {
      case "core.id" => col("core.id").as("eventID")
      case "eventDate.gte" => col("eventDate.gte").as("eventDate")
      case "eventType.concept"=> col("eventType.concept").as("/eventType")
      case x => {
        col("" + x).as(x)
      }
    }.asInstanceOf[Seq[Column]]
  }

  def generateCoreFieldMetaName(field:String): String = {
    field match {
      case "core.id" => "http://rs.tdwg.org/dwc/terms/eventID"
      case "eventDate.gte" => "http://rs.tdwg.org/dwc/terms/eventDate"
      case "eventType.concept" => "http://rs.gbif.org/terms/1.0/eventType"
      case "elevationAccuracy" => "http://rs.gbif.org/terms/1.0/elevationAccuracy"
      case "depthAccuracy" => "http://rs.gbif.org/terms/1.0/depthAccuracy"
      case x => "http://rs.tdwg.org/dwc/terms/" + x
    }
  }

  private def cleanupFileExport(jobID: String, extensionSimpleName: String) = {

    val localFile = new File(workingDirectory + jobID + "/" + extensionSimpleName)

    if (localFile.exists()){
      println("Local file exists = " + localFile.getPath)
    } else {
      val hdfsSiteConf = "/etc/hadoop/conf/hdfs-site.xml"
      val coreSiteConf = "/etc/hadoop/conf/core-site.xml"
      val conf = new Configuration
      conf.addResource(new File(hdfsSiteConf).toURI().toURL())
      conf.addResource(new File(coreSiteConf).toURI().toURL())
      val hdfsPrefixToUse = conf.get("fs.defaultFS")
      val hdfsFs = FileSystem.get(URI.create(hdfsPrefixToUse), conf)

      println("trying to copy to Local = " + localFile.getPath)
      // copy to local file system
      hdfsFs.copyToLocalFile(new Path(hdfsPrefixToUse + workingDirectory + jobID + "/" + extensionSimpleName), new Path(workingDirectory + jobID + "/" + extensionSimpleName))
    }

    // move part-* file to {extension_name}.txt
    println("Cleaning up extension " + extensionSimpleName)

    val file = new File(workingDirectory + jobID + "/" + extensionSimpleName)
    val outputFile = file.listFiles.filter(exportFile => exportFile != null && exportFile.isFile)
      .filter(_.getName.startsWith("part-"))
      .map(_.getPath).toList.head

    // move to sensible name
    FileUtils.moveFile(
      new File(outputFile),
      new File(workingDirectory + jobID + "/" + extensionSimpleName.toLowerCase() + ".txt")
    )

    // remote temporary directory
    FileUtils.forceDelete(new File(workingDirectory + jobID + "/" + extensionSimpleName))
  }

  def generateExtension(extensionUri:String, extensionFields:Array[String]) : Elem = {
    val extensionFileName = extensionUri.substring(extensionUri.lastIndexOf("/") + 1).toLowerCase
    <extension rowType={extensionUri} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
      <files>
        <location>{extensionFileName}.txt</location>
      </files>
      <coreid index="0"/>
      <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>
      {extensionFields.zipWithIndex.map { case (uri, fieldIdx) => {<field index={ (fieldIdx.toInt + 1).toString} term={uri} />} }}
    </extension>
  }

  def createMeta(coreURI:String, coreFields:Seq[String], extensionsForMeta:Map[String, Array[String]]): Elem = {
    val coreFileName = coreURI.substring(coreURI.lastIndexOf("/") + 1).toLowerCase
    val metaXml = <archive xmlns="http://rs.tdwg.org/dwc/text/">
      <core rowType={coreURI} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
        <files>
          <location>{coreFileName}.txt</location>
        </files>
        <id index="0"/>
        {coreFields.zipWithIndex.map { case (uri, index) => <field index={index.toString} term={generateCoreFieldMetaName(uri)}/>} }
      </core>
      {extensionsForMeta.map { case(extensionUri, fields) => generateExtension(extensionUri, fields) } }
    </archive>
    metaXml
  }

  def genericRecordToRow(row:Row, extensionFields: Array[String], sqlType:StructType): Seq[Row] = {
    val coreID = row.get(0).asInstanceOf[String]
    val elements = row.get(1).asInstanceOf[Seq[Map[String, String]] ]
    elements.map(record => {
        val values = extensionFields.map(fieldName => record.getOrElse(fieldName, "")).toArray[Any]
        new GenericRowWithSchema(Array(coreID) ++ values, sqlType)
      }
    )
  }

  def getExtensionList(joined_df:DataFrame, spark:SparkSession): Array[String] = {
    val fieldNameStructureSchema = new StructType()
      .add("fieldName",StringType)

    val extensionsDF = joined_df.select(
      col(s"""extensions""").as("the_extensions")).toDF

    val rowRDD = extensionsDF.rdd.map(row => extensionFieldNameRow(row, fieldNameStructureSchema)).flatMap(list => list)
    val df = spark.sqlContext.createDataFrame(rowRDD , fieldNameStructureSchema)

    val rows = df.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def getExtensionFields(joined_df:DataFrame, extension:String, spark:SparkSession): Array[String] = {
    val fieldNameStructureSchema = new StructType()
      .add("fieldName",StringType)

    val extensionDF = joined_df.select(
      col(s"""extensions.`${extension}`""").
        as("the_extension")).toDF

    val rowRDD = extensionDF.rdd.map(row => genericRecordToFieldNameRow(row, fieldNameStructureSchema)).flatMap(list => list)
    val df = spark.sqlContext.createDataFrame(rowRDD , fieldNameStructureSchema)

    val rows = df.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def extensionFieldNameRow(row:Row, sqlType:StructType): Seq[Row] = {
    val extensionUris = row.get(0).asInstanceOf[Map[String, Any]].keySet
    extensionUris.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType)).toSeq
  }

  def genericRecordToFieldNameRow(row:Row, sqlType:StructType): Seq[Row] = {
    val elements = row.get(0).asInstanceOf[Seq[Map[String, String]] ]
    val fieldNames = elements.map(record => record.keySet).flatten
    fieldNames.distinct.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
  }
}