package org.gbif.pipelines.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.predicate.Predicate;


public class EventQueryVisitorTest {

    public static void main(String[] args) throws Exception {

        String json =
                "{\"type\":\"and\"," +
                "\"predicates\":[" +
                        "{" +
                            "\"type\":\"in\"," +
                            "\"key\":\"STATE_PROVINCE\",\"values\":[\"new south wales\"]," +
                            "\"matchCase\":false" +
                        "}" +
                    "]" +
                "}";
        ObjectMapper om = new ObjectMapper();
        om.addMixIn(SearchParameter.class, EventSearchParameter.class);
        Predicate predicate = om.readValue(json, Predicate.class);
        SparkQueryVisitor v = new EventSparkQueryVisitor();
        String queryString = v.buildQuery(predicate);
        System.out.println(queryString);
    }
}
