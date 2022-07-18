package au.org.ala.pipelines.beam;

import java.util.function.UnaryOperator;
import lombok.Builder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.transforms.common.NotNullOrEmptyFilter;
import org.gbif.pipelines.transforms.converters.ParentEventExpandTransform;
import org.gbif.pipelines.transforms.core.*;

@Builder
public class DerivedMetadata {
  private final Pipeline pipeline;
  private final VerbatimTransform verbatimTransform;
  private final TemporalTransform temporalTransform;
  private final LocationTransform parentLocationTransform;
  private final EventCoreTransform eventCoreTransform;
  private final LocationTransform locationTransform;
  private final PCollection<KV<String, ExtendedRecord>> verbatimCollection;
  private final PCollection<KV<String, TemporalRecord>> temporalCollection;
  private final PCollection<KV<String, LocationRecord>> locationCollection;
  private final PCollection<KV<String, ALATaxonRecord>> taxonCollection;
  private final PCollection<KV<String, EventCoreRecord>> eventCoreCollection;
  private final UnaryOperator<String> occurrencesPathFn;

  /** Calculates the simple Temporal Coverage of an Event. */
  private PCollection<KV<String, EventDate>> temporalCoverage() {
    PCollection<KV<String, TemporalRecord>> eventOccurrenceTemporalCollection =
        pipeline
            .apply(
                "Read occurrence event temporal records", temporalTransform.read(occurrencesPathFn))
            .apply(
                "Remove temporal records with null parent ids",
                Filter.by(NotNullOrEmptyFilter.of(TemporalRecord::getParentId)))
            .apply("Map occurrence events temporal records to KV", temporalTransform.toParentKv());

    // Creates a Map of all events and its sub events
    PCollection<KV<String, TemporalRecord>> temporalRecordsOfSubEvents =
        ParentEventExpandTransform.createTemporalTransform(
                temporalTransform.getTag(), eventCoreTransform.getTag())
            .toSubEventsRecords("Temporal", temporalCollection, eventCoreCollection);

    return PCollectionList.of(temporalCollection)
        .and(eventOccurrenceTemporalCollection)
        .and(temporalRecordsOfSubEvents)
        .apply("Joining temporal records", Flatten.pCollections())
        .apply("Calculate the temporal coverage", Combine.perKey(new TemporalCoverageFn()));
  }

  private PCollection<KV<String, String>> convexHull() {
    PCollection<KV<String, LocationRecord>> eventOccurrenceLocationCollection =
        pipeline
            .apply(
                "Read occurrence events locations", parentLocationTransform.read(occurrencesPathFn))
            .apply(
                "Remove location records with null parent ids",
                Filter.by(NotNullOrEmptyFilter.of(LocationRecord::getParentId)))
            .apply("Map occurrence events locations to KV", parentLocationTransform.toParentKv());

    PCollection<KV<String, LocationRecord>> locationRecordsOfSubEvents =
        ParentEventExpandTransform.createLocationTransform(
                locationTransform.getTag(), eventCoreTransform.getTag())
            .toSubEventsRecords("Location", locationCollection, eventCoreCollection);

    return PCollectionList.of(locationCollection)
        .and(eventOccurrenceLocationCollection)
        .and(locationRecordsOfSubEvents)
        .apply("Joining location records", Flatten.pCollections())
        .apply("Calculate the WKT Convex Hull of all records", Combine.perKey(new ConvexHullFn()));
  }

  private static final TupleTag<Iterable<ALATaxonRecord>> ITERABLE_ALA_TAXON_TAG =
      new TupleTag<Iterable<ALATaxonRecord>>() {};

  PCollection<KV<String, DerivedMetadataRecord>> calculate() {

    PCollection<KV<String, ExtendedRecord>> eventOccurrenceVerbatimCollection =
        pipeline
            .apply("Read event occurrences verbatim", verbatimTransform.read(occurrencesPathFn))
            .apply(
                "Remove verbatim records with null parent ids",
                Filter.by(NotNullOrEmptyFilter.of((ExtendedRecord er) -> er.getParentCoreId())))
            .apply("Map event occurrences verbatim to KV", verbatimTransform.toParentKv());

    return KeyedPCollectionTuple.of(ConvexHullFn.tag(), convexHull())
        .and(TemporalCoverageFn.tag(), temporalCoverage())
        .and(
            verbatimTransform.getTag(),
            PCollectionList.of(eventOccurrenceVerbatimCollection)
                .and(verbatimCollection)
                .apply("Join event and occurrence verbatim records", Flatten.pCollections()))
        .apply("Grouping derived metadata data", CoGroupByKey.create())
        .apply(
            "Creating derived metadata records",
            DerivedMetadataTransform.builder()
                .convexHullTag(ConvexHullFn.tag())
                .temporalCoverageTag(TemporalCoverageFn.tag())
                .extendedRecordTag(verbatimTransform.getTag())
                .build()
                .converter());
  }
}
