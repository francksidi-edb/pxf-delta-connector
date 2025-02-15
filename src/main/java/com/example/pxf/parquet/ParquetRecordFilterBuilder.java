package com.example.pxf.parquet;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.delta.kernel.expressions.*;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Deque;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;

/**
 * This is the implementation of {@link TreeVisitor} for Parquet.
 * <p>
 * The class visits all the {@link Node}s from the expression tree,
 * and builds a simple (single {@link FilterCompat.Filter} class) for
 * {@link org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor} to use for its
 * scan.
 */
public class ParquetRecordFilterBuilder implements TreeVisitor {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Map<String, Type> fields;
    private final List<ColumnDescriptor> columnDescriptors;
    private final Deque<Predicate> filterQueue;

    public static final EnumSet<Operator> SUPPORTED_OPERATORS = EnumSet.of(
            Operator.NOOP,
            Operator.LESS_THAN,
            Operator.GREATER_THAN,
            Operator.LESS_THAN_OR_EQUAL,
            Operator.GREATER_THAN_OR_EQUAL,
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IS_NULL,
            Operator.IS_NOT_NULL,
            // Operator.IN,
            Operator.OR,
            Operator.AND,
            Operator.NOT
    );

    /**
     * Constructor
     *
     * @param columnDescriptors the list of column descriptors
     * @param originalFields    a map of field names to types
     */
    public ParquetRecordFilterBuilder(List<ColumnDescriptor> columnDescriptors, Map<String, Type> originalFields) {
        this.columnDescriptors = columnDescriptors;
        this.filterQueue = new LinkedList<>();
        this.fields = originalFields;
    }

    @Override
    public Node before(Node node, int level) {
        return node;
    }

    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            LOG.info(null == operator ? "null" : "visit node for op " + operator.toString());
            if (!operator.isLogical()) {
                processSimpleColumnOperator(operatorNode);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                processLogicalOperator(operator);
            }
        }
        return node;
    }

    /**
     * Returns the built record predicate
     *
     * @return the built record predicate
     */
    public Predicate getRecordPredicate() {
        Predicate predicate = filterQueue.poll();
        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }
        return predicate;
    }

    private void processLogicalOperator(Operator operator) {
        Predicate right = filterQueue.poll();
        Predicate left = null;

        if (right == null) {
            throw new IllegalStateException("Unable to process logical operator " + operator.toString());
        }

        if (operator == Operator.AND || operator == Operator.OR) {
            left = filterQueue.poll();

            if (left == null) {
                throw new IllegalStateException("Unable to process logical operator " + operator.toString());
            }
        }

        switch (operator) {
            case AND:
                filterQueue.push(new Predicate("AND", left, right));
                break;
            case OR:
                filterQueue.push(new Predicate("OR", left, right));
                break;
            case NOT:
                filterQueue.push(new Predicate("NOT", right));
                break;
        }
    }

    private String convertOperator(Operator opt) {
        switch (opt) {
            case LESS_THAN:
                return "<";
            case GREATER_THAN:
                return ">";
            case LESS_THAN_OR_EQUAL:
                return "<=";
            case GREATER_THAN_OR_EQUAL:
                return ">=";
            case EQUALS:
                return "=";
            case NOT_EQUALS:
                return "!=";
            default:
                throw new IllegalArgumentException("Unsupported operator: " + opt);
        }
    }

    /**
     * Handles simple column-operator-constant expressions.
     *
     * @param operatorNode the operator node
     */
    private void processSimpleColumnOperator(OperatorNode operatorNode) {

        Operator operator = operatorNode.getOperator();
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        OperandNode valueOperand = null;

        if (operator != Operator.IS_NULL && operator != Operator.IS_NOT_NULL) {
            valueOperand = operatorNode.getValueOperand();
            if (valueOperand == null) {
                throw new IllegalArgumentException(
                        String.format("Operator %s does not contain an operand", operator));
            }
        }

        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
        String filterColumnName = columnDescriptor.columnName();
        Type type = fields.get(filterColumnName);

        // INT96 and FIXED_LEN_BYTE_ARRAY cannot be pushed down
        // for more details look at org.apache.parquet.filter2.dictionarylevel.DictionaryFilter#expandDictionary
        // where INT96 and FIXED_LEN_BYTE_ARRAY are not dictionary values
        Predicate simpleFilter;
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32:
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofInt(getIntegerForINT32(type.getLogicalTypeAnnotation(), valueOperand))));
                break;

            case INT64:
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofLong(Long.parseLong(valueOperand.toString()))));
                break;

            case BINARY:
                byte[] value = valueOperand.toString().getBytes();
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofBinary(value)));
                break;

            case BOOLEAN:
                // Boolean does not SupportsLtGt
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofBoolean(Boolean.parseBoolean(valueOperand.toString()))));
                break;

            case FLOAT:
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofFloat(Float.parseFloat(valueOperand.toString()))));
                break;

            case DOUBLE:
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofDouble(Double.parseDouble(valueOperand.toString()))));
                break;
            case FIXED_LEN_BYTE_ARRAY:
                simpleFilter = new Predicate(convertOperator(operator), 
                        Arrays.asList(new Column(filterColumnName), Literal.ofString(valueOperand.toString())));
                break;

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        type.getName(), type.asPrimitiveType().getPrimitiveTypeName()));
        }

        filterQueue.push(simpleFilter);
    }

    private static Integer getIntegerForINT32(LogicalTypeAnnotation logicalTypeAnnotation, OperandNode valueOperand) {
        if (valueOperand == null) return null;
        if (logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
            // Number of days since epoch
            LocalDate localDateValue = LocalDate.parse(valueOperand.toString());
            LocalDate epoch = LocalDate.ofEpochDay(0);
            return (int) ChronoUnit.DAYS.between(epoch, localDateValue);
        }
        return Integer.parseInt(valueOperand.toString());
    }
}
