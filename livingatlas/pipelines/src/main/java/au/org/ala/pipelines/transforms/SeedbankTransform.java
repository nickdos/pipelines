package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.SEEDBANK_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.interpreters.SeedbankInterpreter;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.SeedbankRecord;
import org.gbif.pipelines.transforms.Transform;

public class SeedbankTransform extends Transform<ExtendedRecord, SeedbankRecord> {

  @Builder(buildMethodName = "create")
  private SeedbankTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        SeedbankRecord.class,
        ALARecordTypes.SEEDBANK,
        SeedbankTransform.class.getName(),
        SEEDBANK_RECORDS_COUNT);
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

  public MapElements<SeedbankRecord, KV<String, SeedbankRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, SeedbankRecord>>() {})
        .via((SeedbankRecord lr) -> KV.of(lr.getId(), lr));
  }

  public SeedbankTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<SeedbankRecord> convert(ExtendedRecord source) {
    if (!hasExtension(source, "http://replace-me/terms/seedbankextension")) {
      return Optional.empty();
    }
    Optional<SeedbankRecord> optionalSeedbankRecord =
        SeedbankInterpreter.EXTENDED_HANDLER.convert(source).get();
    optionalSeedbankRecord.ifPresent(sr -> sr.setId(source.getId()));
    return optionalSeedbankRecord;
  }
}
