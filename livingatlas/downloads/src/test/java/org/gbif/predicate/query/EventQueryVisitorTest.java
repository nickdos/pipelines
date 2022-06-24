package org.gbif.predicate.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.junit.Test;


public class EventQueryVisitorTest {

    @Test
    public void testEventQueryFromJSON() throws Exception {

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
        QueryVisitor v = new EventSparkQueryVisitor();
        String queryString = v.buildQuery(predicate);
        System.out.println(queryString);
    }

    @Test
    public void testEventQueryEmptyFromJSON() throws Exception {

        String json = "{\n" +
                "        \"type\": \"and\",\n" +
                "        \"predicates\": [\n" +
                "            {\n" +
                "                \"type\": \"and\",\n" +
                "                \"predicates\": []\n" +
                "            }\n" +
                "        ]\n" +
                "    }";
        ObjectMapper om = new ObjectMapper();
        om.addMixIn(SearchParameter.class, EventSearchParameter.class);
        Predicate predicate = om.readValue(json, Predicate.class);
        QueryVisitor v = new EventSparkQueryVisitor();
        String queryString = v.buildQuery(predicate);
        System.out.println(queryString);
    }
}
