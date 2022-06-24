package org.gbif.predicate.query;

import org.gbif.api.model.predicate.Predicate;

public interface QueryVisitor  {

    String buildQuery(Predicate predicate) throws QueryBuildingException;
}
