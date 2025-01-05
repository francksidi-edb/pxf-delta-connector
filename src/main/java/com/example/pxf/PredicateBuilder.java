package com.example.pxf;
import io.delta.standalone.expressions.*;
import io.delta.standalone.types.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;

import java.util.Map;
import java.util.function.Predicate;
import java.util.HashMap;

import io.delta.standalone.expressions.*;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

import java.util.HashMap;
import java.util.Map;

public class PredicateBuilder {

    private static final Logger LOG = Logger.getLogger(PredicateBuilder.class.getName());
    private final StructType schema;
    private final Map<String, StructField> fieldMap;

    public PredicateBuilder(StructType schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema must not be null for PredicateBuilder");
        }
        this.schema = schema;
        this.fieldMap = initializeFieldMap(schema);
    }

    /**
     * Builds a Delta Standalone Expression based on an OperatorNode filter tree.
     *
     * @param root the root of the filter tree
     * @return a Delta Standalone Expression for filtering
     */
    public Expression buildExpression(OperatorNode root) {
        if (root == null) {
            return null;
        }

        try {
            return buildExpressionFromNode(root);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error building expression from filter tree", e);
            throw new RuntimeException("Failed to build expression from filter tree", e);
        }
    }

    /**
     * Recursively builds an Expression from the parsed OperatorNode.
     *
     * @param node the root OperatorNode of the filter
     * @return an Expression for filtering
     */
    private Expression buildExpressionFromNode(OperatorNode node) {
        Operator operator = node.getOperator();

        if (operator == Operator.AND) {
            return new And(
                buildExpressionFromNode((OperatorNode) node.getLeft()),
                buildExpressionFromNode((OperatorNode) node.getRight())
            );
        } else if (operator == Operator.OR) {
            return new Or(
                buildExpressionFromNode((OperatorNode) node.getLeft()),
                buildExpressionFromNode((OperatorNode) node.getRight())
            );
        } else {
            return createExpressionFromOperation(node);
        }
    }

    /**
     * Creates an Expression from a single OperatorNode.
     *
     * @param node the OperatorNode
     * @return an Expression for the given operation
     */
    private Expression createExpressionFromOperation(OperatorNode node) {
        ColumnIndexOperandNode columnOperand = node.getColumnIndexOperand();
        OperandNode valueOperand = node.getValueOperand();

        int columnIndex = columnOperand.index();
        StructField field = schema.getFields()[columnIndex];
        Object value = parseValue(valueOperand.toString(), field);

        Column column = new Column(field.getName(), field.getDataType());

        switch (node.getOperator()) {
            case EQUALS:
                return new EqualTo(column, createLiteral(value));
            case GREATER_THAN:
                return new GreaterThan(column, createLiteral(value));
            case LESS_THAN:
                return new LessThan(column, createLiteral(value));
            case GREATER_THAN_OR_EQUAL:
                return new GreaterThanOrEqual(column, createLiteral(value));
            case LESS_THAN_OR_EQUAL:
                return new LessThanOrEqual(column, createLiteral(value));
            case NOT_EQUALS:
                return new Not(new EqualTo(column, createLiteral(value)));
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + node.getOperator());
        }
    }

    /**
     * Creates a Literal for a given value.
     *
     * @param value the value for the literal
     * @return a Delta Standalone Literal
     */
    private Literal createLiteral(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null for a literal");
        }

        if (value instanceof Integer) {
            return Literal.of((int) value);
        } else if (value instanceof Double) {
            return Literal.of((double) value);
        } else if (value instanceof String) {
            return Literal.of((String) value);
        } else if (value instanceof Boolean) {
            return Literal.of((boolean) value);
        } else if (value instanceof Long) {
            return Literal.of((long) value);
        } else if (value instanceof java.math.BigDecimal) {
            return Literal.of((java.math.BigDecimal) value);
        } else if (value instanceof java.sql.Date) {
            return Literal.of((java.sql.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
            return Literal.of((java.sql.Timestamp) value);
        } else {
            throw new UnsupportedOperationException("Unsupported literal value type: " + value.getClass().getName());
        }
    }

    /**
     * Parses a value from the filter string into the appropriate data type.
     *
     * @param constantValue the constant value from the filter string
     * @param field         the field metadata
     * @return the parsed value
     */
    private Object parseValue(String constantValue, StructField field) {
        switch (field.getDataType().getTypeName().toLowerCase()) {
            case "integer":
                return Integer.parseInt(constantValue);
            case "double":
                return Double.parseDouble(constantValue);
            case "string":
                return constantValue;
            case "boolean":
                return Boolean.parseBoolean(constantValue);
            case "long":
                return Long.parseLong(constantValue);
            case "decimal":
                return new java.math.BigDecimal(constantValue);
            case "date":
                return java.sql.Date.valueOf(constantValue);
            case "timestamp":
                return java.sql.Timestamp.valueOf(constantValue);
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + field.getDataType().getTypeName());
        }
    }

    /**
     * Initializes a field map for quick lookups during predicate evaluation.
     *
     * @param schema the schema of the Delta table
     * @return a map of column names to StructField objects
     */
    private Map<String, StructField> initializeFieldMap(StructType schema) {
        Map<String, StructField> map = new HashMap<>();
        for (StructField field : schema.getFields()) {
            map.put(field.getName(), field);
        }
        return map;
    }
}

