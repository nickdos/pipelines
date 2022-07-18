package org.gbif.predicate.query;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.*;

public class ALAEventTermsMapper implements SQLTermsMapper<ALAEventSearchParameter> {

  private static final Map<SearchParameter, ? extends Term> PARAM_TO_TERM =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.YEAR, DwcTerm.year)
          .put(ALAEventSearchParameter.MONTH, DwcTerm.month)
          .put(ALAEventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.EVENT_TYPE, GbifTerm.eventType)
          .build();
  private static final Map<SearchParameter, Term> ARRAY_STRING_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .build();

  private static final Map<SearchParameter, Term> DENORMED_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.YEAR, DwcTerm.year)
          .put(ALAEventSearchParameter.MONTH, DwcTerm.month)
          .build();

  @Override
  public Term term(ALAEventSearchParameter searchParameter) {
    return PARAM_TO_TERM.get(searchParameter);
  }

  @Override
  public boolean isArray(ALAEventSearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.containsKey(searchParameter);
  }

  @Override
  public Term getTermArray(ALAEventSearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.get(searchParameter);
  }

  @Override
  public boolean isDenormedTerm(ALAEventSearchParameter searchParameter) {
    return DENORMED_TERMS.containsKey(searchParameter);
  }

  @Override
  public ALAEventSearchParameter getDefaultGadmLevel() {
    return null;
  }
}
