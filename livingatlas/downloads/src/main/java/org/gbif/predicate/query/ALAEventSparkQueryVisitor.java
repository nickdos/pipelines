package org.gbif.predicate.query;

public class ALAEventSparkQueryVisitor extends SQLQueryVisitor<ALAEventSearchParameter> {

  public ALAEventSparkQueryVisitor(SQLTermsMapper<ALAEventSearchParameter> sqlTermsMapper) {
    super(sqlTermsMapper);
  }
}
