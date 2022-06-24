package org.gbif.predicate.query;

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.util.IsoDateParsingUtils.ISO_DATE_FORMATTER;

public abstract class SparkQueryVisitor implements QueryVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(SparkQueryVisitor.class);
    private static final String CONJUNCTION_OPERATOR = " AND ";
    private static final String DISJUNCTION_OPERATOR = " OR ";
    private static final String EQUALS_OPERATOR = " = ";
    private static final String IN_OPERATOR = " IN";
    private static final String GREATER_THAN_OPERATOR = " > ";
    private static final String GREATER_THAN_EQUALS_OPERATOR = " >= ";
    private static final String LESS_THAN_OPERATOR = " < ";
    private static final String LESS_THAN_EQUALS_OPERATOR = " <= ";
    private static final String NOT_OPERATOR = "NOT ";
    private static final String LIKE_OPERATOR = " LIKE ";
    private static final String IS_NOT_NULL_OPERATOR = " IS NOT NULL ";
    private static final String IS_NULL_OPERATOR = " IS NULL ";
    private static final String IS_NOT_NULL_ARRAY_OPERATOR = "(%1$s IS NOT NULL AND size(%1$s) > 0)";
    private static final CharMatcher APOSTROPHE_MATCHER = CharMatcher.is('\'');
    // where query to execute a select all
    private static final String ALL_QUERY = "true";

    private static final Function<Term, String> ARRAY_FN = t ->
            "stringArrayContains(" + HiveColumnsUtils.getHiveQueryColumn(t) + ",'%s',%b)";

    private static final String HIVE_ARRAY_PRE = "ARRAY";

    private static final List<GbifTerm> NUB_KEYS = ImmutableList.of(
            GbifTerm.taxonKey,
            GbifTerm.acceptedTaxonKey,
            GbifTerm.kingdomKey,
            GbifTerm.phylumKey,
            GbifTerm.classKey,
            GbifTerm.orderKey,
            GbifTerm.familyKey,
            GbifTerm.genusKey,
            GbifTerm.subgenusKey,
            GbifTerm.speciesKey
    );

    private StringBuilder builder;

    /**
     * Transforms the value to the Hive statement lower(val).
     */
    private static String toSparkLower(String val) {
        return "lower(" + val + ")";
    }

    private String toSparkSQLField(SearchParameter param, boolean matchCase) {
        return  Optional.ofNullable(term(param)).map( term -> {
            if (term instanceof DwcTerm){
                DwcTerm dwcTerm = (DwcTerm) term;
                String field =  dwcTerm.getGroup() + "." + dwcTerm.simpleName();
                if (String.class.isAssignableFrom(param.type()) && OccurrenceSearchParameter.GEOMETRY != param && !matchCase) {
                    return toSparkLower(field);
                }
                return field;
            }
            return null;
        }).orElseThrow(() ->
                // QueryBuildingException requires an underlying exception
                new IllegalArgumentException("Search parameter " + param + " is not mapped to Hive"));
    }

    private String toSparkSQLDenormField(SearchParameter param, boolean matchCase) {
        return  Optional.ofNullable(term(param)).map( term -> {
            if (term instanceof DwcTerm){
                DwcTerm dwcTerm = (DwcTerm) term;
                String field = "Denorm.parents." + dwcTerm.simpleName();
                if (String.class.isAssignableFrom(param.type()) && OccurrenceSearchParameter.GEOMETRY != param && !matchCase) {
                    return toSparkLower(field);
                }
                return field;
            }
            return null;
        }).orElseThrow(() ->
                // QueryBuildingException requires an underlying exception
                new IllegalArgumentException("Search parameter " + param + " is not mapped to Hive"));
    }

    /**
     * Converts a value to the form expected by Hive based on the OccurrenceSearchParameter.
     * Most values pass by unaltered. Quotes are added for values that need to be quoted, escaping any existing quotes.
     *
     * @param param the type of parameter defining the expected type
     * @param value the original query value
     *
     * @return the converted value expected by Hive
     */
    private static String toHiveValue(SearchParameter param, String value, boolean matchCase) {
        if (Enum.class.isAssignableFrom(param.type())) {
            // all enum parameters are uppercase
            return '\'' + value.toUpperCase() + '\'';
        }

        if (Date.class.isAssignableFrom(param.type())) {
            // use longs for timestamps expressed as ISO dates
            LocalDate ld = IsoDateParsingUtils.parseDate(value);
            Instant i = ld.atStartOfDay(ZoneOffset.UTC).toInstant();
            return String.valueOf(i.toEpochMilli());

        } else if (Number.class.isAssignableFrom(param.type()) || Boolean.class.isAssignableFrom(param.type())) {
            // do not quote numbers
            return value;

        } else {
            // quote value, escape existing quotes
            String strVal =  '\'' + APOSTROPHE_MATCHER.replaceFrom(value, "\\\'") + '\'';
            if (String.class.isAssignableFrom(param.type()) && OccurrenceSearchParameter.GEOMETRY != param && !matchCase) {
                return toSparkLower(strVal);
            }
            return strVal;
        }
    }

    /**
     * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
     * strings that can be used as the <em>WHERE</em> clause for a Hive download.
     *
     * @param predicate to translate
     *
     * @return WHERE clause
     */
    public String buildQuery(Predicate predicate) throws QueryBuildingException {
        String query = ALL_QUERY;
        if (predicate != null) { // null predicate means a SELECT ALL
            builder = new StringBuilder();
            visit(predicate);
            query = builder.toString();
        }

        // Set to null to prevent old StringBuilders hanging around in case this class is reused somewhere else
        builder = null;
        return query;
    }
    public void visit(org.gbif.api.model.predicate.ConjunctionPredicate predicate) throws QueryBuildingException {
        visitCompoundPredicate(predicate, CONJUNCTION_OPERATOR);
    }

    public void visit(org.gbif.api.model.predicate.DisjunctionPredicate predicate) throws QueryBuildingException {
        // See if this disjunction can be simplified into an IN predicate, which is much faster.
        // We could overcomplicate this:
        //   A=1 OR A=2 OR B=3 OR B=4 OR C>5 → A IN(1,2) OR B IN (3,4) OR C>5
        // but that's a very unusual query for us, so we just check for
        // - EqualsPredicates everywhere
        // - on the same search parameter.

        boolean useIn = true;
        Boolean matchCase = null;
        List<String> values = new ArrayList<>();
        SearchParameter parameter = null;

        for (Predicate subPredicate : predicate.getPredicates()) {
            if (subPredicate instanceof org.gbif.api.model.predicate.EqualsPredicate) {
                org.gbif.api.model.predicate.EqualsPredicate equalsSubPredicate = (org.gbif.api.model.predicate.EqualsPredicate) subPredicate;
                if (parameter == null) {
                    parameter = equalsSubPredicate.getKey();
                    matchCase = equalsSubPredicate.isMatchCase();
                } else if (parameter != equalsSubPredicate.getKey() || matchCase != equalsSubPredicate.isMatchCase()) {
                    useIn = false;
                    break;
                }
                values.add(equalsSubPredicate.getValue());
            } else {
                useIn = false;
                break;
            }
        }

        if (useIn) {
            visit(new org.gbif.api.model.predicate.InPredicate(parameter, values, matchCase));
        } else {
            visitCompoundPredicate(predicate, DISJUNCTION_OPERATOR);
        }
    }

    protected abstract Map<SearchParameter, ? extends Term> getParam2Terms();
    public abstract Map<SearchParameter, Term> getArrayTerms();
    public abstract Map<SearchParameter, Term> getDenormedTerms();

    /**
     * Supports all parameters incl taxonKey expansion for higher taxa.
     */
    public void visit(org.gbif.api.model.predicate.EqualsPredicate predicate) throws QueryBuildingException {
//        if (OccurrenceSearchParameter.TAXON_KEY == predicate.getKey()) {
//            appendTaxonKeyFilter(predicate.getValue());
//        } else if (OccurrenceSearchParameter.GADM_GID == predicate.getKey()) {
//            appendGadmGidFilter(predicate.getValue());
//        } else if (OccurrenceSearchParameter.MEDIA_TYPE == predicate.getKey()) {
//            Optional.ofNullable(VocabularyUtils.lookupEnum(predicate.getValue(), MediaType.class))
//                    .ifPresent(mediaType -> builder.append(String.format(ARRAY_FN.apply(GbifTerm.mediaType), mediaType.name(), true)));
//        } else if (OccurrenceSearchParameter.TYPE_STATUS == predicate.getKey()) {
//            Optional.ofNullable(VocabularyUtils.lookupEnum(predicate.getValue(), TypeStatus.class))
//                    .ifPresent(typeStatus -> builder.append(String.format(ARRAY_FN.apply(DwcTerm.typeStatus), typeStatus.name(), true)));
//        } else if (OccurrenceSearchParameter.ISSUE == predicate.getKey()) {
//            builder.append(String.format(ARRAY_FN.apply(GbifTerm.issue), predicate.getValue().toUpperCase(), true));
//        } else
        if (getArrayTerms().containsKey(predicate.getKey())) {
            builder.append(String.format(ARRAY_FN.apply(getArrayTerms().get(predicate.getKey())), predicate.getValue(), predicate.isMatchCase()));
        } else if (TermUtils.isVocabulary(term(predicate.getKey()))) {
            builder.append(String.format(ARRAY_FN.apply(term(predicate.getKey())), predicate.getValue(), true));
        } else if (Date.class.isAssignableFrom(predicate.getKey().type())) {
            // Dates may contain a range even for an EqualsPredicate (e.g. "2000" or "2000-02")
            // The user's query value is inclusive, but the parsed dateRange is exclusive of the
            // upperBound to allow including the day itself.
            //
            // I.e. a predicate value 2000/2005-03 gives a dateRange [2000-01-01,2005-04-01)
            Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());

            if (dateRange.hasLowerBound() || dateRange.hasUpperBound()) {
                builder.append('(');
                if (dateRange.hasLowerBound()) {
                    visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
                    if (dateRange.hasUpperBound()) {
                        builder.append(CONJUNCTION_OPERATOR);
                    }
                }
                if (dateRange.hasUpperBound()) {
                    visitSimplePredicate(predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
                }
                builder.append(')');
            }
        } else {
            visitSimplePredicate(predicate, EQUALS_OPERATOR);
        }
    }

    public void visit(org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate predicate) throws QueryBuildingException {
        if (Date.class.isAssignableFrom(predicate.getKey().type())) {
            // Where the date is a range, consider the "OrEquals" to mean including the whole range.
            // "2000" includes all of 2000.
            Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
            visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
        } else {
            visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR);
        }
    }

    public void visit(org.gbif.api.model.predicate.GreaterThanPredicate predicate) throws QueryBuildingException {
        if (Date.class.isAssignableFrom(predicate.getKey().type())) {
            // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole range.
            // "2000" excludes all of 2000, so the earliest date is 2001-01-01.
            Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
            visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
        } else {
            visitSimplePredicate(predicate, GREATER_THAN_OPERATOR);
        }
    }

    public void visit(org.gbif.api.model.predicate.LessThanOrEqualsPredicate predicate) throws QueryBuildingException {
        if (Date.class.isAssignableFrom(predicate.getKey().type())) {
            // Where the date is a range, consider the "OrEquals" to mean including the whole range.
            // "2000" includes all of 2000, so the latest date is 2001-01-01 (not inclusive).
            Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
            visitSimplePredicate(predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
        } else {
            visitSimplePredicate(predicate, LESS_THAN_EQUALS_OPERATOR);
        }
    }

    public void visit(org.gbif.api.model.predicate.LessThanPredicate predicate) throws QueryBuildingException {
        if (Date.class.isAssignableFrom(predicate.getKey().type())) {
            // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole range.
            // "2000" excludes all of 2000, so the latest date is 2000-01-01 (not inclusive).
            Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
            visitSimplePredicate(predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
        } else {
            visitSimplePredicate(predicate, LESS_THAN_OPERATOR);
        }
    }

    /*
     * For large disjunctions, IN predicates are around 3× faster than a perfectly balanced tree of
     * OR predicates, and around 2× faster than a fairly flat OR query.
     *
     * With Hive 1.3.0, balancing OR queries should be internal to Hive:
     *   https://jira.apache.org/jira/browse/HIVE-11398
     * but it is probably still better to use an IN, which uses a hash table lookup internally:
     *   https://jira.apache.org/jira/browse/HIVE-11415#comment-14651085
     */
    public void visit(org.gbif.api.model.predicate.InPredicate predicate) throws QueryBuildingException {

        boolean isMatchCase = Optional.ofNullable(predicate.isMatchCase()).orElse(Boolean.FALSE);

        if (isHiveArray(predicate.getKey()) || TermUtils.isVocabulary(term(predicate.getKey()))) {
            // Array values must be converted to ORs.
            builder.append('(');
            Iterator<String> iterator = predicate.getValues().iterator();
            while (iterator.hasNext()) {
                // Use the equals predicate to get the behaviour for array.
                visit(new org.gbif.api.model.predicate.EqualsPredicate(predicate.getKey(), iterator.next(), isMatchCase));
                if (iterator.hasNext()) {
                    builder.append(DISJUNCTION_OPERATOR);
                }
            }
            builder.append(')');
        } else {
            builder.append('(');
            builder.append(toSparkSQLField(predicate.getKey(), isMatchCase));
            builder.append(IN_OPERATOR);
            builder.append('(');
            Iterator<String> iterator = predicate.getValues().iterator();
            while (iterator.hasNext()) {
                builder.append(toHiveValue(predicate.getKey(), iterator.next(), isMatchCase));
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
            builder.append(")");

            if (getDenormedTerms().containsKey(predicate.getKey())){
                builder.append(" OR ");
                builder.append('(');

                Iterator<String> iterator2 = predicate.getValues().iterator();
                while (iterator2.hasNext()) {
                    builder.append('(');
                    // FIX ME
                    builder.append("array_contains(" + toSparkSQLDenormField(predicate.getKey(), true) + ",");
                    builder.append(toHiveValue(predicate.getKey(), iterator2.next(), true));
                    builder.append(')');
                    builder.append(')');

                    if (iterator2.hasNext()) {
                        builder.append(" OR ");
                    }
                }
                builder.append(")");


            }
            builder.append(")");
        }

    }

    public void visit(org.gbif.api.model.predicate.LikePredicate predicate) throws QueryBuildingException {
        // Replace % → \% and _ → \_
        // Then replace * → % and ? → _
        org.gbif.api.model.predicate.LikePredicate likePredicate = new org.gbif.api.model.predicate.LikePredicate(
                predicate.getKey(),
                predicate.getValue()
                        .replace("%", "\\%")
                        .replace("_", "\\_")
                        .replace('*', '%')
                        .replace('?', '_'),
                predicate.isMatchCase());

        visitSimplePredicate(likePredicate, LIKE_OPERATOR);
    }

    public void visit(org.gbif.api.model.predicate.NotPredicate predicate) throws QueryBuildingException {
        builder.append(NOT_OPERATOR);
        visit(predicate.getPredicate());
    }

    public void visit(org.gbif.api.model.predicate.IsNotNullPredicate predicate) throws QueryBuildingException {
        builder.append(toSparkSQLField(predicate.getParameter(), true));
        builder.append(IS_NOT_NULL_OPERATOR);
    }

    public void visit(org.gbif.api.model.predicate.IsNullPredicate predicate) throws QueryBuildingException {
        builder.append(toSparkSQLField(predicate.getParameter(), true));
        builder.append(IS_NULL_OPERATOR);
    }

    public void visit(WithinPredicate within) throws QueryBuildingException {
        throw new QueryBuildingException("Not supported");
    }

    /**
     * Builds a list of predicates joined by 'op' statements.
     * The final statement will look like this:
     * <p/>
     * <pre>
     * ((predicate) op (predicate) ... op (predicate))
     * </pre>
     */
    public void visitCompoundPredicate(org.gbif.api.model.predicate.CompoundPredicate predicate, String op) throws QueryBuildingException {
        builder.append('(');
        Iterator<Predicate> iterator = predicate.getPredicates().iterator();
        while (iterator.hasNext()) {
            Predicate subPredicate = iterator.next();
            builder.append('(');
            visit(subPredicate);
            builder.append(')');
            if (iterator.hasNext()) {
                builder.append(op);
            }
        }
        builder.append(')');
    }

    public void visitSimplePredicate(org.gbif.api.model.predicate.SimplePredicate predicate, String op) throws QueryBuildingException {
        if (Number.class.isAssignableFrom(predicate.getKey().type())) {
            if (SearchTypeValidator.isRange(predicate.getValue())) {
                if (Integer.class.equals(predicate.getKey().type())) {
                    visit(toIntegerRangePredicate(SearchTypeValidator.parseIntegerRange(predicate.getValue()), predicate.getKey()));
                } else {
                    visit(toNumberRangePredicate(SearchTypeValidator.parseDecimalRange(predicate.getValue()), predicate.getKey()));
                }
                return;
            }
        }
        builder.append(toSparkSQLField(predicate.getKey(), predicate.isMatchCase()));
        builder.append(op);
        builder.append(toHiveValue(predicate.getKey(), predicate.getValue(), predicate.isMatchCase()));
    }

    public void visitSimplePredicate(org.gbif.api.model.predicate.SimplePredicate predicate, String op, String value) {
        builder.append(toSparkSQLField(predicate.getKey(), predicate.isMatchCase()));
        builder.append(op);
        builder.append(toHiveValue(predicate.getKey(), value, predicate.isMatchCase()));
    }

    /**
     * Determines if the parameter type is a Hive array.
     */
    private boolean isHiveArray(SearchParameter parameter) {
        return HiveColumnsUtils.getHiveType(term(parameter)).startsWith(HIVE_ARRAY_PRE);
    }

    /** Term associated to a search parameter */
    public Term term(SearchParameter parameter) {
        return getParam2Terms().get(parameter);
    }

    /**
     * Converts decimal range into a predicate with the form: field >= range.lower AND field <= range.upper.
     */
    private static Predicate toNumberRangePredicate(Range<Double> range, SearchParameter key) {
        if (!range.hasLowerBound()) {
            return new org.gbif.api.model.predicate.LessThanOrEqualsPredicate(key, String.valueOf(range.upperEndpoint().doubleValue()));
        }
        if (!range.hasUpperBound()) {
            return new org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate(key, String.valueOf(range.lowerEndpoint().doubleValue()));
        }

        ImmutableList<Predicate> predicates = new ImmutableList.Builder<Predicate>()
                .add(new org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate(key, String.valueOf(range.lowerEndpoint().doubleValue())))
                .add(new org.gbif.api.model.predicate.LessThanOrEqualsPredicate(key, String.valueOf(range.upperEndpoint().doubleValue())))
                .build();
        return new org.gbif.api.model.predicate.ConjunctionPredicate(predicates);
    }

    /**
     * Converts integer range into a predicate with the form: field >= range.lower AND field <= range.upper.
     */
    private static Predicate toIntegerRangePredicate(Range<Integer> range, SearchParameter key) {
        if (!range.hasLowerBound()) {
            return new org.gbif.api.model.predicate.LessThanOrEqualsPredicate(key, String.valueOf(range.upperEndpoint().intValue()));
        }
        if (!range.hasUpperBound()) {
            return new org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate(key, String.valueOf(range.lowerEndpoint().intValue()));
        }

        ImmutableList<Predicate> predicates = new ImmutableList.Builder<Predicate>()
                .add(new org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate(key, String.valueOf(range.lowerEndpoint().intValue())))
                .add(new org.gbif.api.model.predicate.LessThanOrEqualsPredicate(key, String.valueOf(range.upperEndpoint().intValue())))
                .build();
        return new org.gbif.api.model.predicate.ConjunctionPredicate(predicates);
    }

    private void visit(Object object) throws QueryBuildingException {
        Method method = null;
        try {
            method = getClass().getMethod("visit", new Class[] {object.getClass()});
        } catch (NoSuchMethodException e) {
            LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
            throw new IllegalArgumentException("Unknown Predicate", e);
        }
        try {
            method.invoke(this, object);
        } catch (IllegalAccessException e) {
            LOG.error("This error shouldn't occur if all visit methods are public. Probably a programming error", e);
            Throwables.propagate(e);
        } catch (InvocationTargetException e) {
            LOG.info("Exception thrown while building the query", e);
            throw new QueryBuildingException(e);
        }
    }
}