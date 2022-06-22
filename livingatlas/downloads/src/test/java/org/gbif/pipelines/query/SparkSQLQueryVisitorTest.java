package org.gbif.pipelines.query;

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;

import java.util.Arrays;


public class SparkSQLQueryVisitorTest {

    public static void main(String[] args) throws Exception {

        InPredicate p = new InPredicate(
                EventSearchParameter.STATE_PROVINCE,
                Arrays.asList("new south wales"),
                false
        );

        ConjunctionPredicate cp = new ConjunctionPredicate(
                Arrays.asList(p)
        );
        SparkSQLQueryVisitor v = new EventQueryVisitor();
        String queryString = v.getSparkSQLQuery(cp);
        System.out.println(queryString);
    }
}
