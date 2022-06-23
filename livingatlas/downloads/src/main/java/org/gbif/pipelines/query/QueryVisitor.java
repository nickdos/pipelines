package org.gbif.pipelines.query;

import org.gbif.api.model.occurrence.predicate.Predicate;

public interface QueryVisitor  {

    String buildQuery(Predicate predicate) throws QueryBuildingException;
}
