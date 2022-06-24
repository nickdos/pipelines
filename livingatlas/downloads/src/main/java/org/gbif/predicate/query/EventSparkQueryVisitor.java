package org.gbif.predicate.query;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

public class EventSparkQueryVisitor extends SparkQueryVisitor {
  private static final Map<SearchParameter, ? extends Term> PARAMS_TO_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(EventSearchParameter.YEAR, DwcTerm.year)
          .put(EventSearchParameter.MONTH, DwcTerm.month)
          .put(EventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(EventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(EventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(EventSearchParameter.EVENT_TYPE, GbifTerm.eventType)
          .build();
  private static final Map<SearchParameter, Term> ARRAY_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(EventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(EventSearchParameter.MEASUREMENT_TYPES, DwcTerm.measurementType)
          .build();

  private static final Map<SearchParameter, Term> DENORMED_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(EventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(EventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(EventSearchParameter.YEAR, DwcTerm.year)
          .put(EventSearchParameter.MONTH, DwcTerm.month)
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
