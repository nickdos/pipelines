package org.gbif.pipelines.query;

import com.google.common.collect.ImmutableMap;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Map;

public class EventSparkQueryVisitor extends SparkQueryVisitor {

    private final static Map<SearchParameter, ? extends Term> PARAMS_TO_TERMS = ImmutableMap.<SearchParameter, Term>builder()
                    .put(EventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
                    .put(EventSearchParameter.EVENT_TYPE, GbifTerm.eventType)
                    .build();
    private final static Map<SearchParameter, Term> ARRAY_TERMS = ImmutableMap.<SearchParameter, Term>builder()
            .put(EventSearchParameter.SAMPLING_PROTOCOL,DwcTerm.samplingProtocol)
            .build();

    @Override
    public Map<SearchParameter, ? extends Term> getParam2Terms() {
        return PARAMS_TO_TERMS;
    }

    public Map<SearchParameter, Term> getArrayTerms() {
            return ARRAY_TERMS;
    }
}
