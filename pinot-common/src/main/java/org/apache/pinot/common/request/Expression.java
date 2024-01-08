/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.15.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.pinot.common.request;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.15.0)", date = "2023-12-07")
public class Expression implements org.apache.thrift.TBase<Expression, Expression._Fields>, java.io.Serializable, Cloneable, Comparable<Expression> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Expression");

  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField FUNCTION_CALL_FIELD_DESC = new org.apache.thrift.protocol.TField("functionCall", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField LITERAL_FIELD_DESC = new org.apache.thrift.protocol.TField("literal", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField IDENTIFIER_FIELD_DESC = new org.apache.thrift.protocol.TField("identifier", org.apache.thrift.protocol.TType.STRUCT, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ExpressionStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ExpressionTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable ExpressionType type; // required
  private @org.apache.thrift.annotation.Nullable Function functionCall; // optional
  private @org.apache.thrift.annotation.Nullable Literal literal; // optional
  private @org.apache.thrift.annotation.Nullable Identifier identifier; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TYPE((short)1, "type"),
    FUNCTION_CALL((short)2, "functionCall"),
    LITERAL((short)3, "literal"),
    IDENTIFIER((short)4, "identifier");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TYPE
          return TYPE;
        case 2: // FUNCTION_CALL
          return FUNCTION_CALL;
        case 3: // LITERAL
          return LITERAL;
        case 4: // IDENTIFIER
          return IDENTIFIER;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.FUNCTION_CALL,_Fields.LITERAL,_Fields.IDENTIFIER};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.ENUM        , "ExpressionType")));
    tmpMap.put(_Fields.FUNCTION_CALL, new org.apache.thrift.meta_data.FieldMetaData("functionCall", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "Function")));
    tmpMap.put(_Fields.LITERAL, new org.apache.thrift.meta_data.FieldMetaData("literal", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "Literal")));
    tmpMap.put(_Fields.IDENTIFIER, new org.apache.thrift.meta_data.FieldMetaData("identifier", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "Identifier")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Expression.class, metaDataMap);
  }

  public Expression() {
  }

  public Expression(
    ExpressionType type)
  {
    this();
    this.type = type;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Expression(Expression other) {
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetFunctionCall()) {
      this.functionCall = new Function(other.functionCall);
    }
    if (other.isSetLiteral()) {
      this.literal = new Literal(other.literal);
    }
    if (other.isSetIdentifier()) {
      this.identifier = new Identifier(other.identifier);
    }
  }

  public Expression deepCopy() {
    return new Expression(this);
  }

  @Override
  public void clear() {
    this.type = null;
    this.functionCall = null;
    this.literal = null;
    this.identifier = null;
  }

  @org.apache.thrift.annotation.Nullable
  public ExpressionType getType() {
    return this.type;
  }

  public void setType(@org.apache.thrift.annotation.Nullable ExpressionType type) {
    this.type = type;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public Function getFunctionCall() {
    return this.functionCall;
  }

  public void setFunctionCall(@org.apache.thrift.annotation.Nullable Function functionCall) {
    this.functionCall = functionCall;
  }

  public void unsetFunctionCall() {
    this.functionCall = null;
  }

  /** Returns true if field functionCall is set (has been assigned a value) and false otherwise */
  public boolean isSetFunctionCall() {
    return this.functionCall != null;
  }

  public void setFunctionCallIsSet(boolean value) {
    if (!value) {
      this.functionCall = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public Literal getLiteral() {
    return this.literal;
  }

  public void setLiteral(@org.apache.thrift.annotation.Nullable Literal literal) {
    this.literal = literal;
  }

  public void unsetLiteral() {
    this.literal = null;
  }

  /** Returns true if field literal is set (has been assigned a value) and false otherwise */
  public boolean isSetLiteral() {
    return this.literal != null;
  }

  public void setLiteralIsSet(boolean value) {
    if (!value) {
      this.literal = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public Identifier getIdentifier() {
    return this.identifier;
  }

  public void setIdentifier(@org.apache.thrift.annotation.Nullable Identifier identifier) {
    this.identifier = identifier;
  }

  public void unsetIdentifier() {
    this.identifier = null;
  }

  /** Returns true if field identifier is set (has been assigned a value) and false otherwise */
  public boolean isSetIdentifier() {
    return this.identifier != null;
  }

  public void setIdentifierIsSet(boolean value) {
    if (!value) {
      this.identifier = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((ExpressionType)value);
      }
      break;

    case FUNCTION_CALL:
      if (value == null) {
        unsetFunctionCall();
      } else {
        setFunctionCall((Function)value);
      }
      break;

    case LITERAL:
      if (value == null) {
        unsetLiteral();
      } else {
        setLiteral((Literal)value);
      }
      break;

    case IDENTIFIER:
      if (value == null) {
        unsetIdentifier();
      } else {
        setIdentifier((Identifier)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TYPE:
      return getType();

    case FUNCTION_CALL:
      return getFunctionCall();

    case LITERAL:
      return getLiteral();

    case IDENTIFIER:
      return getIdentifier();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TYPE:
      return isSetType();
    case FUNCTION_CALL:
      return isSetFunctionCall();
    case LITERAL:
      return isSetLiteral();
    case IDENTIFIER:
      return isSetIdentifier();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof Expression)
      return this.equals((Expression)that);
    return false;
  }

  public boolean equals(Expression that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_functionCall = true && this.isSetFunctionCall();
    boolean that_present_functionCall = true && that.isSetFunctionCall();
    if (this_present_functionCall || that_present_functionCall) {
      if (!(this_present_functionCall && that_present_functionCall))
        return false;
      if (!this.functionCall.equals(that.functionCall))
        return false;
    }

    boolean this_present_literal = true && this.isSetLiteral();
    boolean that_present_literal = true && that.isSetLiteral();
    if (this_present_literal || that_present_literal) {
      if (!(this_present_literal && that_present_literal))
        return false;
      if (!this.literal.equals(that.literal))
        return false;
    }

    boolean this_present_identifier = true && this.isSetIdentifier();
    boolean that_present_identifier = true && that.isSetIdentifier();
    if (this_present_identifier || that_present_identifier) {
      if (!(this_present_identifier && that_present_identifier))
        return false;
      if (!this.identifier.equals(that.identifier))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetType()) ? 131071 : 524287);
    if (isSetType())
      hashCode = hashCode * 8191 + type.getValue();

    hashCode = hashCode * 8191 + ((isSetFunctionCall()) ? 131071 : 524287);
    if (isSetFunctionCall())
      hashCode = hashCode * 8191 + functionCall.hashCode();

    hashCode = hashCode * 8191 + ((isSetLiteral()) ? 131071 : 524287);
    if (isSetLiteral())
      hashCode = hashCode * 8191 + literal.hashCode();

    hashCode = hashCode * 8191 + ((isSetIdentifier()) ? 131071 : 524287);
    if (isSetIdentifier())
      hashCode = hashCode * 8191 + identifier.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(Expression other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetType(), other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetFunctionCall(), other.isSetFunctionCall());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFunctionCall()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.functionCall, other.functionCall);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetLiteral(), other.isSetLiteral());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLiteral()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.literal, other.literal);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetIdentifier(), other.isSetIdentifier());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIdentifier()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.identifier, other.identifier);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Expression(");
    boolean first = true;

    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (isSetFunctionCall()) {
      if (!first) sb.append(", ");
      sb.append("functionCall:");
      if (this.functionCall == null) {
        sb.append("null");
      } else {
        sb.append(this.functionCall);
      }
      first = false;
    }
    if (isSetLiteral()) {
      if (!first) sb.append(", ");
      sb.append("literal:");
      if (this.literal == null) {
        sb.append("null");
      } else {
        sb.append(this.literal);
      }
      first = false;
    }
    if (isSetIdentifier()) {
      if (!first) sb.append(", ");
      sb.append("identifier:");
      if (this.identifier == null) {
        sb.append("null");
      } else {
        sb.append(this.identifier);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetType()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'type' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExpressionStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExpressionStandardScheme getScheme() {
      return new ExpressionStandardScheme();
    }
  }

  private static class ExpressionStandardScheme extends org.apache.thrift.scheme.StandardScheme<Expression> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Expression struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = org.apache.pinot.common.request.ExpressionType.findByValue(iprot.readI32());
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // FUNCTION_CALL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.functionCall = new Function();
              struct.functionCall.read(iprot);
              struct.setFunctionCallIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LITERAL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.literal = new Literal();
              struct.literal.read(iprot);
              struct.setLiteralIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // IDENTIFIER
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.identifier = new Identifier();
              struct.identifier.read(iprot);
              struct.setIdentifierIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Expression struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeI32(struct.type.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.functionCall != null) {
        if (struct.isSetFunctionCall()) {
          oprot.writeFieldBegin(FUNCTION_CALL_FIELD_DESC);
          struct.functionCall.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.literal != null) {
        if (struct.isSetLiteral()) {
          oprot.writeFieldBegin(LITERAL_FIELD_DESC);
          struct.literal.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.identifier != null) {
        if (struct.isSetIdentifier()) {
          oprot.writeFieldBegin(IDENTIFIER_FIELD_DESC);
          struct.identifier.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExpressionTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExpressionTupleScheme getScheme() {
      return new ExpressionTupleScheme();
    }
  }

  private static class ExpressionTupleScheme extends org.apache.thrift.scheme.TupleScheme<Expression> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Expression struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.type.getValue());
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetFunctionCall()) {
        optionals.set(0);
      }
      if (struct.isSetLiteral()) {
        optionals.set(1);
      }
      if (struct.isSetIdentifier()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetFunctionCall()) {
        struct.functionCall.write(oprot);
      }
      if (struct.isSetLiteral()) {
        struct.literal.write(oprot);
      }
      if (struct.isSetIdentifier()) {
        struct.identifier.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Expression struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.type = org.apache.pinot.common.request.ExpressionType.findByValue(iprot.readI32());
      struct.setTypeIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.functionCall = new Function();
        struct.functionCall.read(iprot);
        struct.setFunctionCallIsSet(true);
      }
      if (incoming.get(1)) {
        struct.literal = new Literal();
        struct.literal.read(iprot);
        struct.setLiteralIsSet(true);
      }
      if (incoming.get(2)) {
        struct.identifier = new Identifier();
        struct.identifier.read(iprot);
        struct.setIdentifierIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

