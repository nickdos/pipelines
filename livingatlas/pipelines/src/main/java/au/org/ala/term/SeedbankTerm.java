package au.org.ala.term;

import java.net.URI;
import org.gbif.dwc.terms.Term;

/** Set of terms in use by seedbank */
public enum SeedbankTerm implements Term {
  adjustedGermination, // - check numeric
  darkHours, // - check numeric
  dateCollected, // - check valid date
  dateInStorage, // - check valid date
  dayTemp,
  lightHours, // - check numeric
  nightTemp, // - check numeric
  numberFull, // - check numeric
  numberGerminated, // - check numeric
  numberPlantsSampled, // - check numeric
  purityDebris,
  sampleSize, // - check numeric
  sampleWeight, // - check numeric
  testLengthInDays, // - check numeric
  thousandSeedWeight; // - check numeric

  private static final URI NS_URI = URI.create("http://REPLACE-ME/terms/");

  SeedbankTerm() {}

  public String simpleName() {
    return this.name();
  }

  @Override
  public String prefixedName() {
    return Term.super.prefixedName();
  }

  @Override
  public String qualifiedName() {
    return Term.super.qualifiedName();
  }

  @Override
  public boolean isClass() {
    return false;
  }

  public String toString() {
    return this.prefixedName();
  }

  public String prefix() {
    return "seedbank";
  }

  public URI namespace() {
    return NS_URI;
  }
}
