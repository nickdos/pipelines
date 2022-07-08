package org.gbif.pipelines.events

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.gbif.api.model.common.search.SearchParameter
import org.gbif.api.model.predicate.Predicate
import org.gbif.dwc.terms.DwcTerm
import org.gbif.predicate.query.{ALAEventSearchParameter, ALAEventSparkQueryVisitor}

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.xml.Elem

/**
 * Pipeline uses Spark SQL to produce a DwCA Archive.
 */
object DownloadDwCAPipeline {

  val workingDirectory = "/tmp/pipelines-export/"

  val SKIPPED_FIELDS = List(
    "id",
    "parentId",
    "issues",
    "created",
    "hasGeospatialIssue",
    "hasCoordinate",
    "repatriated"
  )

  // Test with some sample data
  def main(args: Array[String]): Unit = {

    if (args.length == 3) {
      println("Please supply <DatasetId> <HDFS_BasePath> <Attempt> <JobID> <LocalSpark> <PredicateQuery>")
      println("e.g. dr18391 hdfs://localhost:9000/pipelines-data 1 job-123 false {}")
      return;
    }

//    val args = Array("dr18391", "hdfs://localhost:9000/pipelines-data", "1", "generate", "false", "{}")
    // val args = Array("dr18391", "hdfs:///pipelines-data", "1", "generate", "false", "{}")

    import java.util.UUID
    val datasetId = args(0)
    val hdfsPath = args(1)
    val attempt = args(2)
    val jobID = if (args(3) != "generate") {
      args(3)
    } else {
      UUID.randomUUID.toString
    }
    val localTest = args(4).toBoolean
    val predicateQueryJSON = args(5)

    // Process the query filter
    val queryFilter = if (predicateQueryJSON != "{}"){
      val om = new ObjectMapper();
      val unescaped = predicateQueryJSON.replaceAll("\\\\", "")
      om.addMixIn(classOf[SearchParameter], classOf[ALAEventSearchParameter]);
      val predicate = om.readValue(unescaped, classOf[Predicate]);
      val v = new ALAEventSparkQueryVisitor();
      v.buildQuery(predicate);
    } else {
      ""
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
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/event/*.avro").as("Core")

    System.out.println("Load location")
    val locationDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/location/*.avro").as("Location")

    System.out.println("Load temporal")
    val temporalDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/temporal/*.avro").as("Temporal")

    System.out.println("Load denorm")
    val denormDF = spark.read.format("avro").
      load(s"${hdfsPath}/${datasetId}/${attempt}/event/event_hierarchy/*.avro").as("Denorm")

    System.out.println("Join")
    val downloadDF = eventCoreDF.
      join(locationDF, col("Core.id") === col("Location.id"), "inner").
      join(temporalDF, col("Core.id") === col("Temporal.id"), "inner").
      join(denormDF, col("Core.id") === col("Denorm.id"), "inner")

    // get a list columns
    val exportPath = workingDirectory + jobID + s"/Event/"

    val filterDownloadDF = if (queryFilter != ""){
      // filter "coreTerms", "extensions"
      downloadDF.filter(queryFilter)
    } else {
      downloadDF
    }

    // generate interpreted event export
    System.out.println("Export interpreted event data")
    val (eventExportDF, eventFields) = generateInterpretedExportDF(filterDownloadDF)
    eventExportDF
      .write
      .option("header","true")
      .option("sep","\t")
      .mode("overwrite")
      .csv(exportPath)

    cleanupFileExport(jobID, "Event")

    // load the verbatim DF
    val verbatimDF = spark.read.format("avro").
        load(s"${hdfsPath}/${datasetId}/${attempt}/verbatim.avro").as("Verbatim")

    // export the supplied core verbatim
    val verbatimCoreFields = exportVerbatimCore(jobID, spark, verbatimDF, filterDownloadDF)

    // export the supplied extensions verbatim
    val verbatimExtensionsForMeta = exportVerbatimExtensions(jobID, spark, verbatimDF, filterDownloadDF)

    // export interpreted occurrence
    val occurrenceFields = exportInterpretedOccurrence(datasetId, hdfsPath, attempt, jobID, spark, filterDownloadDF, verbatimExtensionsForMeta.keySet)

    // package ZIP
    createZip(
      datasetId,
      jobID,
      DwcTerm.Event.qualifiedName(),
      eventFields,
      occurrenceFields,
      verbatimCoreFields,
      verbatimExtensionsForMeta
    )

    println("Export complete")
  }

  private def exportVerbatimCore(jobID: String, spark: SparkSession, verbatimDF: Dataset[Row], filterDownloadDF: DataFrame) = {

    val dfWithExtensions = filterDownloadDF.
      join(verbatimDF, col("Core.id") === col("Verbatim.id"), "inner")

    val coreFields = getCoreFields(dfWithExtensions, spark).filter(!_.endsWith("eventID"))
    val columns = Array(col("core.id").as("eventID")) ++ coreFields.map {
      fieldName => col("coreTerms.`" + fieldName+"`").as(fieldName.substring(fieldName.lastIndexOf("/") + 1))
    }
    val coreForExportDF = dfWithExtensions.select(columns:_*)
    coreForExportDF.select("*")
      .coalesce(1).write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .csv(workingDirectory + jobID + "/Verbatim_Event" )
    cleanupFileExport(jobID, "Verbatim_Event")

   coreFields
  }

  private def exportVerbatimExtensions(jobID: String, spark: SparkSession, verbatimDF: Dataset[Row], filterDownloadDF: DataFrame) = {

    val dfWithExtensions = filterDownloadDF.
      join(verbatimDF, col("Core.id") === col("Verbatim.id"), "inner")

    val extensionsForMeta = scala.collection.mutable.Map[String, Array[String]]()

    // get list of extensions for this dataset
    val extensionList = getExtensionList(dfWithExtensions, spark)

    // export all supplied extensions verbatim
    extensionList.foreach(extensionURI => {

      val extensionFields = getExtensionFields(dfWithExtensions, extensionURI, spark)

      val arrayStructureSchema = {
        var builder = new StructType().add("id", StringType)
        extensionFields.foreach { fieldName =>
          val isURI = fieldName.lastIndexOf("/") > 0
          val simpleName = if (isURI) {
            fieldName.substring(fieldName.lastIndexOf("/") + 1)
          } else {
            fieldName
          }
          builder = builder.add(simpleName, StringType)
        }
        builder
      }

      val extensionDF = dfWithExtensions.select(
        col("Core.id").as("id"),
        col(s"""extensions.`${extensionURI}`""").as("the_extension")
      ).toDF
      val rowRDD = extensionDF.rdd.map(row => genericRecordToRow(row, extensionFields, arrayStructureSchema)).flatMap(list => list)
      val extensionForExportDF = spark.sqlContext.createDataFrame(rowRDD, arrayStructureSchema)

      // filter "coreTerms", "extensions"
      val extensionSimpleName = extensionURI.substring(extensionURI.lastIndexOf("/") + 1)

      extensionForExportDF.select("*")
        .coalesce(1).write
        .option("header", "true")
        .option("sep", "\t")
        .mode("overwrite")
        .csv(workingDirectory + jobID + "/Verbatim_" + extensionSimpleName)

      cleanupFileExport(jobID, "Verbatim_" + extensionSimpleName)
      extensionsForMeta(extensionURI) = extensionFields
    })

    extensionsForMeta.toMap
  }

  private def exportInterpretedOccurrence(datasetId: String, hdfsPath: String, attempt: String, jobID: String,
                                          spark: SparkSession, filterDownloadDF: DataFrame,
                                          extensionList: Set[String]) :Array[String] = {
    // If an occurrence extension was supplied
    if (extensionList.contains(DwcTerm.Occurrence.qualifiedName())) {

      System.out.println("Load basic  for occurrences")
      val occBasicDF = spark.read.format("avro").
        load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/basic/*.avro").as("OccBasic").filter("parentId is NOT NULL")

      System.out.println("Load occurrences")
      val occTaxonDF = spark.read.format("avro").
        load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/ala_taxonomy/*.avro").as("OccTaxon").filter("parentId is NOT NULL")

      System.out.println("Load temporal  for occurrences")
      val occTemporalDF = spark.read.format("avro").
        load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/temporal/*.avro").as("OccTemporal").filter("parentId is NOT NULL")

      System.out.println("Load location for occurrences")
      val occLocationDF = spark.read.format("avro").
        load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/location/*.avro").as("OccLocation").filter("parentId is NOT NULL")

      System.out.println("Create occurrence join DF")
      val occDF = occBasicDF.
          join(occTaxonDF, col("OccBasic.id") === col("OccTaxon.id"), "inner").
          join(occLocationDF, col("OccBasic.id") === col("OccLocation.id"), "inner").
          join(occTemporalDF, col("OccBasic.id") === col("OccTemporal.id"), "inner")

      val joinOccDF = filterDownloadDF.select(col("Core.id")).
        join(occDF, col("Core.id") === col("OccBasic.parentId"), "inner")

      System.out.println("Generate interpreted occurrence DF for export")
      val (exportDF, fields) = generateInterpretedExportDF(joinOccDF)

      System.out.println("Export interpreted occurrence data")
      exportDF.write
        .option("header", "true")
        .option("sep", "\t")
        .mode("overwrite")
        .csv(workingDirectory + jobID + "/Occurrence")

      cleanupFileExport(jobID, "Occurrence")

      fields
    } else {
      Array[String]()
    }
  }

  def generateInterpretedExportDF(df:DataFrame): (DataFrame, Array[String]) = {

    val primitiveFields = df.schema.fields.filter(structField => {
      if (structField.dataType.isInstanceOf[StringType]
        || structField.dataType.isInstanceOf[DoubleType]
        || structField.dataType.isInstanceOf[BooleanType]
      ) true else false
    })

    val stringArrayFields = df.schema.fields.filter(structField => {
      if (structField.dataType.isInstanceOf[ArrayType]) {
        val arrayType = structField.dataType.asInstanceOf[ArrayType]
        if (arrayType.elementType.isInstanceOf[StringType]) {
          true
        } else {
          false
        }
      } else {
        false
      }
    })

    val exportFields = (primitiveFields.map { field => field.name } ++ stringArrayFields.map { field => field.name })
      .filter(!SKIPPED_FIELDS.contains(_))

    val fields = Array(col("core.id").as("eventID")) ++ exportFields.map(col(_))

    var occDFCoalesce = df.select(fields: _*).coalesce(1)

    stringArrayFields.foreach(arrayField => {
      occDFCoalesce = occDFCoalesce.withColumn(arrayField.name, col(arrayField.name).cast("string"))
    })

    (occDFCoalesce, Array("id") ++ exportFields)
  }

  private def createZip(datasetId: String,
                        jobID: String,
                        coreTermType: String,
                        coreFieldList:Array[String],
                        occurrenceFieldList:Array[String],
                        verbatimCoreFields:Array[String],
                        extensionsForMeta: Map[String, Array[String]]) = {
    // write the XML
    import scala.collection.JavaConversions._

    val metaXml = createMeta(coreTermType, coreFieldList, occurrenceFieldList, verbatimCoreFields, extensionsForMeta)

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
  }

  def generateFieldColumns(fields:Seq[String]): Seq[Column] = {
    fields.map {
      case "core.id" => col("core.id").as("id")
      case "eventDate.gte" => col("eventDate.gte").as("eventDate")
      case "eventType.concept"=> col("eventType.concept").as("/eventType")
      case x => {
        col("" + x).as(x)
      }
    }
  }

  def generateCoreFieldMetaName(field:String): String = {
    if (field.startsWith("http")){
      field
    } else {
      field match {
        case "id" => "http://rs.tdwg.org/dwc/terms/eventID"
        case "core.id" => "http://rs.tdwg.org/dwc/terms/eventID"
        case "eventDate.gte" => "http://rs.tdwg.org/dwc/terms/eventDate"
        case "eventType.concept" => "http://rs.gbif.org/terms/1.0/eventType"
        case "elevationAccuracy" => "http://rs.gbif.org/terms/1.0/elevationAccuracy"
        case "depthAccuracy" => "http://rs.gbif.org/terms/1.0/depthAccuracy"
        case x => "http://rs.tdwg.org/dwc/terms/" + x
      }
    }
  }

  /**
   * Clean up the file export, moving to sensible files names instead of the
   * part-* file name generated by spark.
   *
   * @param jobID
   * @param extensionSimpleName
   */
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

      println("Trying to copy to Local = " + localFile.getPath)
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

  def generateInterpretedExtension(extensionUri:String, extensionFields:Array[String]) : Elem = {
    val extensionFileName = extensionUri.substring(extensionUri.lastIndexOf("/") + 1).toLowerCase
    <extension rowType={extensionUri} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
      <files>
        <location>{extensionFileName}.txt</location>
      </files>
      <coreid index="0"/>
      {extensionFields.zipWithIndex.map { case (uri, fieldIdx) => {<field index={ fieldIdx.toString } term={generateCoreFieldMetaName(uri)} />} }}
    </extension>
  }

  def generateVerbatimExtension(extensionUri:String, extensionFields:Array[String]) : Elem = {
    val extensionFileName = extensionUri.substring(extensionUri.lastIndexOf("/") + 1).toLowerCase
    <extension rowType={extensionUri} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
      <files>
        <location>verbatim_{extensionFileName}.txt</location>
      </files>
      <coreid index="0"/>
      { if (false) <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/> }
      <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>
      {extensionFields.zipWithIndex.map { case (uri, fieldIdx) => {<field index={ (fieldIdx.toInt + 1).toString} term={generateCoreFieldMetaName(uri)} />} }}
    </extension>
  }

  def createMeta(coreURI:String, coreFields:Seq[String], occurrenceFields:Array[String], verbatimCoreFields:Array[String], extensionsForMeta:Map[String, Array[String]]): Elem = {
    val coreFileName = coreURI.substring(coreURI.lastIndexOf("/") + 1).toLowerCase
    val metaXml = <archive xmlns="http://rs.tdwg.org/dwc/text/">
      <core rowType={coreURI} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
        <files>
          <location>{coreFileName}.txt</location>
        </files>
        <id index="0"/>
        {coreFields.zipWithIndex.map { case (uri, index) => <field index={index.toString} term={generateCoreFieldMetaName(uri)}/>} }
      </core>
      { generateInterpretedExtension(DwcTerm.Occurrence.qualifiedName(), occurrenceFields) }
      { generateVerbatimExtension(DwcTerm.Event.qualifiedName(), verbatimCoreFields) }
      { extensionsForMeta.map { case(extensionUri, fields) => generateVerbatimExtension(extensionUri, fields) } }
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

    val fieldNameDF = spark.sqlContext.createDataFrame(rowRDD , fieldNameStructureSchema)

    val rows = fieldNameDF.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def getCoreFields(verbatimDF:DataFrame, spark:SparkSession): Array[String] = {

    import org.apache.spark.sql.types._
    val fieldNameStructureSchema:StructType = new StructType().add("fieldName", StringType)

    val coreDF = verbatimDF.select(
      col(s"""coreTerms""").
        as("coreTerms")).toDF

    val rowRDD = coreDF.rdd.map { row =>
      coreRecordToFieldNameRow(row, fieldNameStructureSchema)
    }.flatMap(list => list)

    val df = spark.sqlContext.createDataFrame(rowRDD, fieldNameStructureSchema)
    val rows = df.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def getExtensionFields(joined_df:DataFrame, extension:String, spark:SparkSession): Array[String] = {
    val fieldNameStructureSchema = new StructType()
      .add("fieldName", StringType)

    val extensionDF = joined_df.select(
      col(s"""extensions.`${extension}`""").
        as("the_extension")).toDF

    val rowRDD = extensionDF.rdd.map(row => genericRecordToFieldNameRow(row, fieldNameStructureSchema)).flatMap(list => list)
    val df = spark.sqlContext.createDataFrame(rowRDD , fieldNameStructureSchema)
    val rows = df.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def coreRecordToFieldNameRow(row:Row, sqlType:StructType): Seq[Row] = {
    val extensionUris = row.get(0).asInstanceOf[Map[String, Any]].keySet
    extensionUris.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType)).toSeq
  }

  def extensionFieldNameRow(row:Row, sqlType:StructType): Seq[Row] = {
    val extensionUris = row.get(0).asInstanceOf[Map[String, Any]].keySet
    extensionUris.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType)).toSeq
  }

  def genericCoreRecordToFieldNameRow(row:Row, sqlType:StructType): Set[GenericRowWithSchema] = {
    val elements = row.get(0).asInstanceOf[Map[String, String] ]
    val fieldNames = elements.keySet.flatten
    fieldNames.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
  }

  def genericRecordToFieldNameRow(row:Row, sqlType:StructType): Seq[Row] = {
    val elements = row.get(0).asInstanceOf[Seq[Map[String, String]] ]
    val fieldNames = elements.map(record => record.keySet).flatten
    fieldNames.distinct.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
  }
}