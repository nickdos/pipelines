package au.org.ala.pipelines.interpreters;

import au.org.ala.term.SeedbankTerm;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
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

  private final ExtensionInterpretation.TargetHandler<SeedbankRecord> handler =
      ExtensionInterpretation.extension("http://replace-me/terms/seedbankextension")
          .to(SeedbankRecord::new)
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
          .map(SeedbankTerm.thousandSeedWeight, SeedbankInterpreter::setThousandSeedWeight)
          .map(SeedbankTerm.seedPerGram, SeedbankInterpreter::setSeedPerGram)
          .map(SeedbankTerm.purity, SeedbankInterpreter::setPurity)
          .map(SeedbankTerm.viability, SeedbankInterpreter::setViability)
          .map(SeedbankTerm.relativeHumidity, SeedbankInterpreter::setRelativeHumidity)
          .map(SeedbankTerm.storageTemp, SeedbankInterpreter::setStorageTemp)
          .map(SeedbankTerm.germinateRate, SeedbankInterpreter::setGerminateRate)
          .map(SeedbankTerm.numberEmpty, SeedbankInterpreter::setNumberEmpty)
          .map(SeedbankTerm.numberTested, SeedbankInterpreter::setNumberTested)
          .map(SeedbankTerm.dateInStorage, this::interpretDateInStorage)
          .map(SeedbankTerm.dateCollected, this::interpretDateCollected)
          .map(SeedbankTerm.testDateStarted, this::interpretTestDateStarted);

  @Builder(buildMethodName = "create")
  private SeedbankInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.temporalParser = TemporalParser.create(orderings);
    this.preprocessDateFn = preprocessDateFn;
  }

  public void interpret(ExtendedRecord er, SeedbankRecord sr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(sr);
    ExtensionInterpretation.Result<SeedbankRecord> result = handler.convert(er);
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

  public static void setSeedPerGram(SeedbankRecord sr, String value) {
    try {
      sr.setSeedPerGram(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setPurity(SeedbankRecord sr, String value) {
    try {
      sr.setPurity(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setViability(SeedbankRecord sr, String value) {
    try {
      sr.setViability(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setRelativeHumidity(SeedbankRecord sr, String value) {
    try {
      sr.setRelativeHumidity(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setStorageTemp(SeedbankRecord sr, String value) {
    try {
      sr.setStorageTemp(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setGerminateRate(SeedbankRecord sr, String value) {
    try {
      sr.setGerminateRate(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberEmpty(SeedbankRecord sr, String value) {
    try {
      sr.setNumberEmpty(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public static void setNumberTested(SeedbankRecord sr, String value) {
    try {
      sr.setNumberTested(Double.parseDouble(value));
    } catch (Exception e) {
      // do nothing
    }
  }

  public void interpretDateCollected(SeedbankRecord sr, String dateCollected) {
    if (dateCollected != null) {
      String normalised =
          Optional.ofNullable(preprocessDateFn)
              .map(x -> x.apply(dateCollected))
              .orElse(dateCollected);
      OccurrenceParseResult<TemporalAccessor> parsed = temporalParser.parseRecordedDate(normalised);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setDateCollected);
      }
    }
  }

  public void interpretDateInStorage(SeedbankRecord sr, String dateInStorage) {
    if (dateInStorage != null) {
      String normalised =
          Optional.ofNullable(preprocessDateFn)
              .map(x -> x.apply(dateInStorage))
              .orElse(dateInStorage);
      OccurrenceParseResult<TemporalAccessor> parsed = temporalParser.parseRecordedDate(normalised);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setDateInStorage);
      }
    }
  }

  public void interpretTestDateStarted(SeedbankRecord sr, String testDateStarted) {
    if (testDateStarted != null) {
      String normalizedDate =
          Optional.ofNullable(preprocessDateFn)
              .map(x -> x.apply(testDateStarted))
              .orElse(testDateStarted);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseRecordedDate(normalizedDate);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(ta -> TemporalAccessorUtils.toDate(ta).getTime())
            .ifPresent(sr::setTestDateStarted);
      }
    }
  }
}
