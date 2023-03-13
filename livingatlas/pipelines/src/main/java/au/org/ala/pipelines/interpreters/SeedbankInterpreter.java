package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValue;

import au.org.ala.term.SeedbankTerm;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.SeedbankRecord;

public class SeedbankInterpreter {

  private final TemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  public static final ExtensionInterpretation.TargetHandler<SeedbankRecord> EXTENDED_HANDLER =
      ExtensionInterpretation.extension("http://replace-me/terms/seedbankextension")
          .to(SeedbankRecord::new)
          .map("id", SeedbankRecord::setId)
          .map(SeedbankTerm.adjustedGermination, SeedbankInterpreter::setAdjustedGermination)
          .map(SeedbankTerm.darkHours, SeedbankInterpreter::setDarkHours)
          .map(SeedbankTerm.dayTemp, SeedbankInterpreter::setDayTemp)
          .map(SeedbankTerm.lightHours, SeedbankInterpreter::setLightHours)
          .map(SeedbankTerm.numberFull, SeedbankInterpreter::setNumberFull)
          .map(SeedbankTerm.numberGerminated, SeedbankInterpreter::setNumberGerminated)
          .map(SeedbankTerm.numberPlantsSampled, SeedbankInterpreter::setNumberPlantsSampled)
          .map(SeedbankTerm.purityDebris, SeedbankInterpreter::setPurityDebris)
          .map(SeedbankTerm.sampleSize, SeedbankInterpreter::setSampleSize)
          .map(SeedbankTerm.sampleWeight, SeedbankInterpreter::setSampleWeight)
          .map(SeedbankTerm.testLengthInDays, SeedbankInterpreter::setTestLengthInDays)
          .map(SeedbankTerm.thousandSeedWeight, SeedbankInterpreter::setThousandSeedWeight);

  @Builder(buildMethodName = "create")
  private SeedbankInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.preprocessDateFn = preprocessDateFn;
    this.temporalParser = TemporalParser.create(orderings);
  }

  public static void setSampleWeight(SeedbankRecord sr, String value) {
    try {
      sr.setSampleWeight(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setSampleSize(SeedbankRecord sr, String value) {
    try {
      sr.setSampleSize(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setAdjustedGermination(SeedbankRecord sr, String value) {
    try {
      sr.setAdjustedGermination(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setDarkHours(SeedbankRecord sr, String value) {
    try {
      sr.setDarkHours(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setDayTemp(SeedbankRecord sr, String value) {
    try {
      sr.setDayTemp(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setLightHours(SeedbankRecord sr, String value) {
    try {
      sr.setLightHours(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNightTemp(SeedbankRecord sr, String value) {
    try {
      sr.setNightTemp(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberFull(SeedbankRecord sr, String value) {
    try {
      sr.setNumberFull(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberGerminated(SeedbankRecord sr, String value) {
    try {
      sr.setNumberGerminated(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberPlantsSampled(SeedbankRecord sr, String value) {
    try {
      sr.setNumberPlantsSampled(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setPurityDebris(SeedbankRecord sr, String value) {
    try {
      sr.setPurityDebris(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setTestLengthInDays(SeedbankRecord sr, String value) {
    try {
      sr.setTestLengthInDays(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setThousandSeedWeight(SeedbankRecord sr, String value) {
    try {
      sr.setThousandSeedWeight(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public void interpretDateCollected(ExtendedRecord er, SeedbankRecord sr) {
    if (hasValue(er, SeedbankTerm.dateCollected)) {
      String value = extractValue(er, SeedbankTerm.dateCollected);
      String normalizedValue =
          Optional.ofNullable(preprocessDateFn).map(x -> x.apply(value)).orElse(value);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseRecordedDate(normalizedValue);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setDateCollected);
      }
    }
  }

  public void interpretDateInStorage(ExtendedRecord er, SeedbankRecord sr) {
    if (hasValue(er, SeedbankTerm.dateInStorage)) {
      String value = extractValue(er, SeedbankTerm.dateInStorage);
      String normalizedValue =
          Optional.ofNullable(preprocessDateFn).map(x -> x.apply(value)).orElse(value);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseRecordedDate(normalizedValue);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setDateInStorage);
      }
    }
  }
}
