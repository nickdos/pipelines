package org.gbif.predicate.query;

import java.util.function.Function;
import org.gbif.dwc.terms.Term;

public class ALAEventSparkQueryVisitor extends SQLQueryVisitor<ALAEventSearchParameter> {

  protected SQLTermsMapper<ALAEventSearchParameter> sqlTermsMapper;

  private static final Function<Term, String> ARRAY_FN =
      t -> "array_contains(" + SQLColumnsUtils.getSQLQueryColumn(t) + ",'%s')";

  public ALAEventSparkQueryVisitor(SQLTermsMapper<ALAEventSearchParameter> sqlTermsMapper) {
    super(sqlTermsMapper);
    this.sqlTermsMapper = sqlTermsMapper;
  }

  @Override
  public Function<Term, String> getArrayFn() {
    return ARRAY_FN;
  }

  @Override
  protected boolean isSQLArray(ALAEventSearchParameter parameter) {
    return sqlTermsMapper.isArray(parameter);
  }
}
