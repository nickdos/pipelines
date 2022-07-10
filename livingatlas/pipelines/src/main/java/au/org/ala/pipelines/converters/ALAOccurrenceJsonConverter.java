package au.org.ala.pipelines.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.GbifClassification;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
@Builder
public class ALAOccurrenceJsonConverter {

  private final ALAMetadataRecord metadata;
  private final ALAUUIDRecord uuid;
  private final BasicRecord basic;
  private final TemporalRecord temporal;
  private final LocationRecord location;
  private final ALATaxonRecord taxon;
  private final MultimediaRecord multimedia;
  private final ExtendedRecord verbatim;

  public OccurrenceJsonRecord convert() {

    OccurrenceJsonRecord.Builder builder = OccurrenceJsonRecord.newBuilder();
    // FIX ME
    builder.setId(uuid.getUuid());
    builder.setCreated(uuid.getFirstLoaded().toString());
    builder.setGbifId(1);
    mapMetadataRecord(builder);
    mapBasicRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapTaxonRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);

    return builder.build();
  }

  public String toJson() {
    return convert().toString();
  }

  private void mapMetadataRecord(OccurrenceJsonRecord.Builder builder) {
    builder
        .setDatasetKey(metadata.getDataResourceUid())
        .setDatasetTitle(metadata.getDataResourceName());
  }

  private void mapBasicRecord(OccurrenceJsonRecord.Builder builder) {

    // Simple
    builder
        .setBasisOfRecord(basic.getBasisOfRecord())
        .setSex(basic.getSex())
        .setIndividualCount(basic.getIndividualCount())
        .setTypeStatus(basic.getTypeStatus())
        .setTypifiedName(basic.getTypifiedName())
        .setSampleSizeValue(basic.getSampleSizeValue())
        .setSampleSizeUnit(basic.getSampleSizeUnit())
        .setOrganismQuantity(basic.getOrganismQuantity())
        .setOrganismQuantityType(basic.getOrganismQuantityType())
        .setRelativeOrganismQuantity(basic.getRelativeOrganismQuantity())
        .setReferences(basic.getReferences())
        .setIdentifiedBy(basic.getIdentifiedBy())
        .setRecordedBy(basic.getRecordedBy())
        .setOccurrenceStatus(basic.getOccurrenceStatus())
        .setDatasetID(basic.getDatasetID())
        .setDatasetName(basic.getDatasetName())
        .setOtherCatalogNumbers(basic.getOtherCatalogNumbers())
        .setPreparations(basic.getPreparations())
        .setSamplingProtocol(basic.getSamplingProtocol());

    //         Agent
    builder
        .setIdentifiedByIds(JsonConverter.convertAgentList(basic.getIdentifiedByIds()))
        .setRecordedByIds(JsonConverter.convertAgentList(basic.getRecordedByIds()));

    // VocabularyConcept
    JsonConverter.convertVocabularyConcept(basic.getLifeStage()).ifPresent(builder::setLifeStage);
    JsonConverter.convertVocabularyConcept(basic.getEstablishmentMeans())
        .ifPresent(builder::setEstablishmentMeans);
    JsonConverter.convertVocabularyConcept(basic.getDegreeOfEstablishment())
        .ifPresent(builder::setDegreeOfEstablishment);
    JsonConverter.convertVocabularyConcept(basic.getPathway()).ifPresent(builder::setPathway);

    // License
    JsonConverter.convertLicense(basic.getLicense()).ifPresent(builder::setLicense);

    // Multivalue fields
    JsonConverter.convertToMultivalue(basic.getRecordedBy())
        .ifPresent(builder::setRecordedByJoined);
    JsonConverter.convertToMultivalue(basic.getIdentifiedBy())
        .ifPresent(builder::setIdentifiedByJoined);
    JsonConverter.convertToMultivalue(basic.getPreparations())
        .ifPresent(builder::setPreparationsJoined);
    JsonConverter.convertToMultivalue(basic.getSamplingProtocol())
        .ifPresent(builder::setSamplingProtocolJoined);
    JsonConverter.convertToMultivalue(basic.getOtherCatalogNumbers())
        .ifPresent(builder::setOtherCatalogNumbersJoined);
  }

  protected static Optional<org.gbif.pipelines.io.avro.json.VocabularyConcept>
      convertVocabularyConcept(org.gbif.pipelines.io.avro.VocabularyConcept concepts) {
    if (concepts == null) {
      return Optional.empty();
    }
    return Optional.of(
        org.gbif.pipelines.io.avro.json.VocabularyConcept.newBuilder()
            .setConcept(concepts.getConcept())
            .setLineage(concepts.getLineage())
            .build());
  }

  private void mapTemporalRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setYear(temporal.getYear())
        .setMonth(temporal.getMonth())
        .setDay(temporal.getDay())
        .setStartDayOfYear(temporal.getStartDayOfYear())
        .setEndDayOfYear(temporal.getEndDayOfYear())
        .setModified(temporal.getModified())
        .setDateIdentified(temporal.getDateIdentified());

    JsonConverter.convertEventDate(temporal.getEventDate()).ifPresent(builder::setEventDate);
    JsonConverter.convertEventDateSingle(temporal).ifPresent(builder::setEventDateSingle);
  }

  private void mapLocationRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setContinent(location.getContinent())
        .setWaterBody(location.getWaterBody())
        .setCountry(location.getCountry())
        .setCountryCode(location.getCountryCode())
        .setPublishingCountry(location.getPublishingCountry())
        .setStateProvince(location.getStateProvince())
        .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
        .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
        .setElevation(location.getElevation())
        .setElevationAccuracy(location.getElevationAccuracy())
        .setDepth(location.getDepth())
        .setDepthAccuracy(location.getDepthAccuracy())
        .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
        .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
        .setMaximumDistanceAboveSurfaceInMeters(location.getMaximumDistanceAboveSurfaceInMeters())
        .setMinimumDistanceAboveSurfaceInMeters(location.getMinimumDistanceAboveSurfaceInMeters())
        .setCoordinateUncertaintyInMeters(location.getCoordinateUncertaintyInMeters())
        .setCoordinatePrecision(location.getCoordinatePrecision())
        .setHasCoordinate(location.getHasCoordinate())
        .setRepatriated(location.getRepatriated())
        .setHasGeospatialIssue(location.getHasGeospatialIssue())
        .setLocality(location.getLocality())
        .setFootprintWKT(location.getFootprintWKT());

    // Coordinates
    Double decimalLongitude = location.getDecimalLongitude();
    Double decimalLatitude = location.getDecimalLatitude();
    if (decimalLongitude != null && decimalLatitude != null) {
      builder
          .setDecimalLatitude(decimalLatitude)
          .setDecimalLongitude(decimalLongitude)
          // geo_point
          .setCoordinates(JsonConverter.convertCoordinates(decimalLongitude, decimalLatitude))
          // geo_shape
          .setScoordinates(JsonConverter.convertScoordinates(decimalLongitude, decimalLatitude));
    }

    JsonConverter.convertGadm(location.getGadm()).ifPresent(builder::setGadm);
  }

  private void mapTaxonRecord(OccurrenceJsonRecord.Builder builder) {
    // Set  GbifClassification
    builder.setGbifClassification(convertClassification(verbatim, taxon));
  }

  public static GbifClassification convertClassification(
      ExtendedRecord verbatim, ALATaxonRecord taxon) {

    GbifClassification.Builder classificationBuilder =
        GbifClassification.newBuilder()
            //
            // .setClassification(JsonConverter.convertRankedNames(taxon.getClassification()))
            .setTaxonID(taxon.getTaxonConceptID());

    //
    // JsonConverter.convertRankedName(taxon.getUsage()).ifPresent(classificationBuilder::setUsage);

    //    JsonConverter.convertRankedName(taxon.getAcceptedUsage())
    //            .ifPresent(classificationBuilder::setAcceptedUsage);
    classificationBuilder.setAcceptedUsage(
        org.gbif.pipelines.io.avro.json.RankedName.newBuilder()
            .setKey(-1)
            .setName(taxon.getScientificName())
            .setRank(taxon.getTaxonRank())
            .build());

    //
    // classificationBuilder.setUsageParsedName(org.gbif.pipelines.io.avro.json.ParsedName.newBuilder()
    //            .setName(taxon.getScientificName())
    //            .setRank(taxon.getTaxonRank())
    //            .build());

    //    JsonConverter.convertDiagnostic(taxon.getDiagnostics())
    //            .ifPresent(classificationBuilder::setDiagnostics);

    //    JsonConverter.convertParsedName(taxon.getUsageParsedName())
    //            .ifPresent(classificationBuilder::setUsageParsedName);

    //    JsonConverter.convertGenericName(taxon)
    //            .ifPresent(
    //                    genericName -> {
    //                      if (classificationBuilder.getUsageParsedName() != null) {
    //
    // classificationBuilder.getUsageParsedName().setGenericName(genericName);
    //                      }
    //                    });

    //    convertClassificationPath(taxon)
    //            .ifPresent(classificationBuilder::setClassificationPath);

    classificationBuilder.setKingdom(taxon.getKingdom());
    classificationBuilder.setKingdomKey(taxon.getKingdomID());
    classificationBuilder.setPhylum(taxon.getPhylum());
    classificationBuilder.setPhylumKey(taxon.getPhylumID());
    classificationBuilder.setClass$(taxon.getClasss());
    classificationBuilder.setClassKey(taxon.getClassID());
    classificationBuilder.setOrder(taxon.getOrder());
    classificationBuilder.setOrderKey(taxon.getOrderID());
    classificationBuilder.setFamily(taxon.getFamily());
    classificationBuilder.setFamilyKey(taxon.getFamilyID());
    classificationBuilder.setGenus(taxon.getGenus());
    classificationBuilder.setGenusKey(taxon.getGenusID());
    classificationBuilder.setSpecies(taxon.getSpecies());
    classificationBuilder.setSpeciesKey(taxon.getSpeciesID());

    // Raw to index classification
    extractOptValue(verbatim, DwcTerm.taxonID).ifPresent(classificationBuilder::setTaxonID);
    extractOptValue(verbatim, DwcTerm.scientificName)
        .ifPresent(classificationBuilder::setVerbatimScientificName);

    return classificationBuilder.build();
  }

  //  public static Optional<org.gbif.pipelines.io.avro.json.RankedName> convertName(String rank,
  // String name, String taxonKey) {
  //    return org.gbif.pipelines.io.avro.json.RankedName.newBuilder()
  //                                    .setName(name)
  //                                    .setRank(rank)
  //                                    .setKey(taxonKey)
  //                                    .build());
  //  }

  private void mapMultimediaRecord(OccurrenceJsonRecord.Builder builder) {
    //    builder
    //        .setMultimediaItems(JsonConverter.convertMultimediaList(multimedia))
    //        .setMediaTypes(JsonConverter.convertMultimediaType(multimedia))
    //        .setMediaLicenses(JsonConverter.convertMultimediaLicense(multimedia));
  }

  private void mapExtendedRecord(OccurrenceJsonRecord.Builder builder) {

    builder
        .setId(verbatim.getId())
        .setAll(JsonConverter.convertFieldAll(verbatim))
        .setExtensions(JsonConverter.convertExtensions(verbatim))
        .setVerbatim(JsonConverter.convertVerbatimRecord(verbatim));

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.recordNumber).ifPresent(builder::setRecordNumber);
    extractOptValue(verbatim, DwcTerm.organismID).ifPresent(builder::setOrganismId);
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventId);
    extractOptValue(verbatim, DwcTerm.parentEventID).ifPresent(builder::setParentEventId);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.collectionCode).ifPresent(builder::setCollectionCode);
    extractOptValue(verbatim, DwcTerm.catalogNumber).ifPresent(builder::setCatalogNumber);
    extractOptValue(verbatim, DwcTerm.occurrenceID).ifPresent(builder::setOccurrenceId);
  }
}
