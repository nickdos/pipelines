package org.gbif.predicate.query;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
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
          .put(ALAEventSearchParameter.taxonKey, GbifTerm.taxonKey)
          .put(ALAEventSearchParameter.year, DwcTerm.year)
          .put(ALAEventSearchParameter.month, DwcTerm.month)
          .put(ALAEventSearchParameter.stateProvince, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.countryCode, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.samplingProtocol, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.eventTypeHierarchy, ALASearchTerm.eventTypeHierarchy)
          .put(ALAEventSearchParameter.eventHierarchy, ALASearchTerm.eventHierarchy)
          .put(ALAEventSearchParameter.datasetKey, GbifTerm.datasetKey)
          .build();
  private static final Map<SearchParameter, Term> ARRAY_STRING_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.samplingProtocol, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.stateProvince, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.countryCode, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.eventTypeHierarchy, ALASearchTerm.eventTypeHierarchy)
          .put(ALAEventSearchParameter.eventHierarchy, ALASearchTerm.eventHierarchy)
          .put(ALAEventSearchParameter.datasetKey, GbifTerm.datasetKey)
          .build();

  private static final Map<SearchParameter, Term> DENORMED_TERMS = Collections.EMPTY_MAP;

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
