package org.gbif.predicate.query;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

public class ALAEventSparkQueryVisitor extends SparkQueryVisitor {
  private static final Map<SearchParameter, ? extends Term> PARAMS_TO_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.YEAR, DwcTerm.year)
          .put(ALAEventSearchParameter.MONTH, DwcTerm.month)
          .put(ALAEventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.EVENT_TYPE, GbifTerm.eventType)
          .build();
  private static final Map<SearchParameter, Term> ARRAY_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.MEASUREMENT_TYPES, DwcTerm.measurementType)
          .build();

  private static final Map<SearchParameter, Term> DENORMED_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.YEAR, DwcTerm.year)
          .put(ALAEventSearchParameter.MONTH, DwcTerm.month)
          .build();

  @Override
  public Map<SearchParameter, ? extends Term> getParam2Terms() {
    return PARAMS_TO_TERMS;
  }

  public Map<SearchParameter, Term> getArrayTerms() {
    return ARRAY_TERMS;
  }

  public Map<SearchParameter, Term> getDenormedTerms() {
    return DENORMED_TERMS;
  }
}
