package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;

import au.org.ala.pipelines.interpreters.ALATemporalInterpreter;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * ALA version of the Beam level transformations for the DWC Event, reads an avro, writes an avro,
 * maps from value to keyValue and transforms form {@link ExtendedRecord} to {@link TemporalRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
 * as a source and {@link ALATemporalInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#event">https://dwc.tdwg.org/terms/#event</a>
 */
public class ALATemporalTransform extends Transform<ExtendedRecord, TemporalRecord> {

  private final SerializableFunction<String, String> preprocessDateFn;
  private final List<DateComponentOrdering> orderings;
  private TemporalInterpreter temporalInterpreter;

  @Builder(buildMethodName = "create")
  private ALATemporalTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        TemporalRecord.class,
        RecordType.TEMPORAL,
        ALATemporalTransform.class.getName(),
        TEMPORAL_RECORDS_COUNT);
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (temporalInterpreter == null) {
      temporalInterpreter =
          TemporalInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getParentId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> asKv(boolean useParent) {
    return MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
        .via((TemporalRecord tr) -> KV.of(useParent ? tr.getParentId() : tr.getId(), tr));
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> toKv() {
    return asKv(false);
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getParentId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> toParentKv() {
    return asKv(true);
  }

  public ALATemporalTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup can be applied only to void method */
  public ALATemporalTransform init() {
    setup();
    return this;
  }

  @Override
  public Optional<TemporalRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                TemporalRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(temporalInterpreter::interpretTemporal)
        .via(temporalInterpreter::interpretModified)
        .via(temporalInterpreter::interpretDateIdentified)
        .via(ALATemporalInterpreter::checkRecordDateQuality)
        .via(ALATemporalInterpreter::checkDateIdentified)
        .via(ALATemporalInterpreter::checkGeoreferencedDate)
        .via(ALATemporalInterpreter::checkDatePrecision)
        .via(TemporalInterpreter::setParentId)
        .getOfNullable();
  }
}
