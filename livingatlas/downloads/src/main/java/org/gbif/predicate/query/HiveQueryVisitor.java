package org.gbif.predicate.query;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;

import java.util.function.Function;

public abstract class HiveQueryVisitor extends SQLQueryVisitor {

  private static final Function<Term, String> ARRAY_FN =
      t -> "stringArrayContains(" + HiveColumnsUtils.getHiveQueryColumn(t) + ",'%s',%b)";

  @Override
  public Function<Term, String> getArrayFn(){
      return ARRAY_FN;
  }
}
