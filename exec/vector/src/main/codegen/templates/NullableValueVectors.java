/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;
import org.apache.drill.exec.vector.NullableVarCharVector.Accessor;

import io.netty.buffer.DrillBuf;

import java.lang.Override;
import java.lang.UnsupportedOperationException;
import java.util.Set;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}Vector" />
<#assign valuesName = "${minor.class}Vector" />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<@pp.changeOutputFile name="/org/apache/drill/exec/vector/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${className} extends BaseDataValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, NullableVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

  private final FieldReader reader = new Nullable${minor.class}ReaderImpl(Nullable${minor.class}Vector.this);

  private final MaterializedField bitsField = MaterializedField.create("$bits$", Types.required(MinorType.UINT1));

  /**
   * Set value flag. Meaning:
   * <ul>
   * <li>0: value is not set (value is null).</li>
   * <li>1: value is set (value is not null).</li>
   * </ul>
   * That is, a 1 means that the values vector has a value. 0
   * means that the vector is null. Thus, all values start as
   * not set (null) and must be explicitly set (made not null).
   */

  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);
  private final ${valuesName} values = new ${minor.class}Vector(field, allocator);
  private final Mutator mutator      = new MutatorImpl();
  private final Accessor accessor    = new AccessorImpl();

  <#if type.major == "VarLen" && minor.class == "VarChar">
  private final Mutator dupMutator   = new DupValsOnlyMutator();
  /** Accessor instance for duplicate values vector */
  private final Accessor dupAccessor = new DupValsOnlyAccessor();
  /** Optimization for cases where all values are identical */
  private boolean duplicateValuesOnly;
  /** logical number of values */
  private int logicalNumValues;
  /** logical value capacity */
  private int logicalValueCapacity;
  /** Mutator instance for duplicate values vector */

  /** true if this vector holds the same value albeit repeated */
  public boolean isDuplicateValsOnly() {
    return duplicateValuesOnly;
  }

  /**
   * Sets this vector duplicate values mode; the {@link #clear()} method wil also be called as a side effect
   *  of this operation
   */
  public void setDuplicateValsOnly(boolean valsOnly) {
    clear();
    duplicateValuesOnly = valsOnly;
  }

  /** {@inheritDoc} */
  @Override
  public int getValueCapacity(){
    if (!isDuplicateValsOnly()) {
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }
    return logicalValueCapacity;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
    if (isDuplicateValsOnly()) {
      logicalNumValues     = 0;
      logicalValueCapacity = 0;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    assert valueCount >= 0;

    if (valueCount == 0) {
      return 0;
    }
    if (!isDuplicateValsOnly()) {
      return values.getBufferSizeFor(valueCount) + bits.getBufferSizeFor(valueCount);
    }
    return values.getBufferSizeFor(1) + bits.getBufferSizeFor(1);
  }

  /** {@inheritDoc} */
  @Override
  public ${valuesName} getValuesVector() {
    if (!isDuplicateValsOnly()) {
      return values;
    }
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public void setInitialCapacity(int numRecords) {
    assert numRecords >= 0;
    if (!isDuplicateValsOnly()) {
      bits.setInitialCapacity(numRecords);
      values.setInitialCapacity(numRecords);
    } else {
      bits.setInitialCapacity(numRecords > 0 ? 1 : 0);
      values.setInitialCapacity(numRecords > 0 ? 1 : 0);
      logicalValueCapacity = numRecords;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Accessor getAccessor() {
    if (!isDuplicateValsOnly()) {
      return accessor;
    }
    return dupAccessor;
  }

  /** {@inheritDoc} */
  @Override
  public Mutator getMutator() {
    if (!isDuplicateValsOnly()) {
      return mutator;
    }
    return dupMutator;
  }

  public void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from) {
    // TODO - Salim: handle the use-case where from-vector is dup-vals whereas this-vector is not; then remove this check
    if (isDuplicateValsOnly() != from.isDuplicateValsOnly()) {
      throw new UnsupportedOperationException();
    }
    final Accessor fromAccessor = from.getAccessor();
    if (!isDuplicateValsOnly()) {
      if (!fromAccessor.isNull(fromIndex)) {
        getMutator().set(thisIndex, fromAccessor.get(fromIndex));
      }
    } else {
      if (logicalNumValues == from.logicalNumValues) {
        return; // NOOP as we only need to copy one entry
      }
      getMutator().setValueCount(from.logicalNumValues);
      setInitialCapacity(from.logicalValueCapacity);
      if (!fromAccessor.isNull(fromIndex)) {
        getMutator().set(0, fromAccessor.get(0));
      }
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from) {
    assert !isDuplicateValsOnly(); // dup-vector currently only supported on nullable-var-char vectors

    mutator.fillEmpties(thisIndex);
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafe(thisIndex, 1);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from) {
    // TODO - Salim: handle the use-case where from-vector is dup-vals whereas this-vector is not; then remove this check
    if (isDuplicateValsOnly() != from.isDuplicateValsOnly()) {
      throw new UnsupportedOperationException();
    }
    if (!isDuplicateValsOnly()) {
      mutator.fillEmpties(thisIndex);
      bits.copyFromSafe(fromIndex, thisIndex, from.bits);
      values.copyFromSafe(fromIndex, thisIndex, from.values);
    } else {
      if (logicalNumValues == from.logicalNumValues) {
        return; // noop as we only need to copy one entry
      }
      getMutator().setValueCount(from.logicalNumValues);
      setInitialCapacity(from.logicalValueCapacity);
      final Accessor fromAccessor = from.getAccessor();
      if (!fromAccessor.isNull(fromIndex)) {
        getMutator().set(0, fromAccessor.get(0));
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    if (!isDuplicateValsOnly()) {
      Nullable${minor.class}Vector fromVector = (Nullable${minor.class}Vector) from;
      // This method is to be called only for loading the vector
      // sequentially, so there should be no empties to fill.
      bits.copyFromSafe(fromIndex, toIndex, fromVector.bits);
      values.copyFromSafe(fromIndex, toIndex, fromVector.values);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void exchange(ValueVector other) {
    if (!isDuplicateValsOnly()) {
      ${className} target = (${className}) other;
      bits.exchange(target.bits);
      values.exchange(target.values);
      mutator.exchange(other.getMutator());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  <#if type.major != "VarLen">
  @Override
  public void toNullable(ValueVector nullableVector) {
    exchange(nullableVector);
    clear();
  }
  </#if>

  /** {@inheritDoc} */
  @Override
  public SerializedField.Builder getMetadataBuilder() {
    if (!isDuplicateValsOnly()) {
      return super.getMetadataBuilder()
          .addChild(bits.getMetadata())
          .addChild(values.getMetadata());
    } else {
      return super.getMetadataBuilder()
        .setIsDup(true)
        .setLogicalValueCount(logicalNumValues)
        .addChild(bits.getMetadata())
        .addChild(values.getMetadata());
    }
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
      clear();
    this.duplicateValuesOnly  = metadata.getIsDup();
    this.logicalNumValues     = metadata.getLogicalValueCount();
    this.logicalValueCapacity = metadata.getLogicalValueCount();

    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buffer);

    final int capacity                = buffer.capacity();
    final int bitsLength              = bitsField.getBufferLength();
    final SerializedField valuesField = metadata.getChild(1);
    values.load(valuesField, buffer.slice(bitsLength, capacity - bitsLength));
    }

  <#else>
  /** true if this vector holds the same value albeit repeated */
  boolean isDuplicateValsOnly() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int getValueCapacity(){
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
  }

  /** {@inheritDoc} */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return values.getBufferSizeFor(valueCount) +
           bits.getBufferSizeFor(valueCount);
  }

  /** {@inheritDoc} */
  @Override
  public ${valuesName} getValuesVector() { return values; }

  /** {@inheritDoc} */
  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    values.setInitialCapacity(numRecords);
  }

  /** {@inheritDoc} */
  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  /** {@inheritDoc} */
  @Override
  public Mutator getMutator(){
    return mutator;
  }

  public void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    final Accessor fromAccessor = from.getAccessor();
    if (!fromAccessor.isNull(fromIndex)) {
      mutator.set(thisIndex, fromAccessor.get(fromIndex));
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafe(thisIndex, 1);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
  }

  /** {@inheritDoc} */
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    Nullable${minor.class}Vector fromVector = (Nullable${minor.class}Vector) from;
    <#if type.major == "VarLen">

    // This method is to be called only for loading the vector
    // sequentially, so there should be no empties to fill.

    </#if>
    bits.copyFromSafe(fromIndex, toIndex, fromVector.bits);
    values.copyFromSafe(fromIndex, toIndex, fromVector.values);
  }

  /** {@inheritDoc} */
  @Override
  public void exchange(ValueVector other) {
    ${className} target = (${className}) other;
    bits.exchange(target.bits);
    values.exchange(target.values);
    mutator.exchange(other.getMutator());
  }

  /** {@inheritDoc} */
  @Override
  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
      .addChild(bits.getMetadata())
      .addChild(values.getMetadata());
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    clear();
    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buffer);

    final int capacity                = buffer.capacity();
    final int bitsLength              = bitsField.getBufferLength();
    final SerializedField valuesField = metadata.getChild(1);
    values.load(valuesField, buffer.slice(bitsLength, capacity - bitsLength));
  }

  </#if>

  public ${className}(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  /** {@inheritDoc} */
  @Override
  public FieldReader getReader(){
    return reader;
  }

  /** {@inheritDoc} */
  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final DrillBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), values.getBuffers(false), DrillBuf.class);
    if (clear) {
      for (final DrillBuf buffer:buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  /** {@inheritDoc} */
  @Override
  public int getBufferSize(){
    return values.getBufferSize() + bits.getBufferSize();
  }

  /** {@inheritDoc} */
  @Override
  public DrillBuf getBuffer() {
    return values.getBuffer();
  }

  /** {@inheritDoc} */
  @Override
  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = values.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return success;
  }

  /** {@inheritDoc} */
  @Override
  public void collectLedgers(Set<BufferLedger> ledgers) {
    bits.collectLedgers(ledgers);
    values.collectLedgers(ledgers);
  }

  /** {@inheritDoc} */
  @Override
  public int getPayloadByteCount(int valueCount) {
    // For nullable, we include all values, null or not, in computing
    // the value length.
    return bits.getPayloadByteCount(valueCount) + values.getPayloadByteCount(valueCount);
  }

  <#if type.major == "VarLen">
  /** {@inheritDoc} */
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    try {
      values.allocateNew(totalBytes, valueCount);
      bits.allocateNew(valueCount);
    } catch(RuntimeException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /** {@inheritDoc} */
  @Override
  public int getByteCapacity(){
    return values.getByteCapacity();
  }

  /** {@inheritDoc} */
  @Override
  public int getCurrentSizeInBytes(){
    return values.getCurrentSizeInBytes();
  }

  <#else>
  /** {@inheritDoc} */
  @Override
  public void allocateNew(int valueCount) {
    try {
      values.allocateNew(valueCount);
      bits.allocateNew(valueCount);
    } catch(OutOfMemoryException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /** {@inheritDoc} */
  @Override
  public void zeroVector() {
    bits.zeroVector();
    values.zeroVector();
  }
  </#if>

  /** {@inheritDoc} */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator){
    return new TransferImpl(getField(), allocator);
  }

  /** {@inheritDoc} */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  /** {@inheritDoc} */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Nullable${minor.class}Vector) to);
  }

  public void transferTo(Nullable${minor.class}Vector target){
    <#if type.major == "VarLen" && minor.class == "VarChar">
    if (isDuplicateValsOnly()) {
      if (!target.isDuplicateValsOnly()) {
        throw new UnsupportedOperationException();
      }
      target.logicalNumValues     = logicalNumValues;
      target.logicalValueCapacity = logicalValueCapacity;
    }
    </#if>
    bits.transferTo(target.bits);
    values.transferTo(target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = mutator.lastSet;
    </#if>
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, Nullable${minor.class}Vector target) {
    <#if type.major == "VarLen" && minor.class == "VarChar">
    if (isDuplicateValsOnly()) {
      if (!target.isDuplicateValsOnly() || startIndex > 0) {
        throw new UnsupportedOperationException();
      }
      target.logicalNumValues     = logicalNumValues;
      target.logicalValueCapacity = logicalValueCapacity;
      startIndex                  = 0;
      length                      = 1;
    }
    </#if>
    bits.splitAndTransferTo(startIndex, length, target.bits);
    values.splitAndTransferTo(startIndex, length, target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = length - 1;
    </#if>
  }

  public ${minor.class}Vector convertToRequiredVector(){
    ${minor.class}Vector v = new ${minor.class}Vector(getField().getOtherNullableVersion(), allocator);
    if (v.data != null) {
      v.data.release(1);
    }
    v.data = values.data;
    v.data.retain(1);
    clear();
    return v;
  }

  /** {@inheritDoc} */
  @Override
  public UInt1Vector getBitsVector() { return bits; }

// ----------------------------------------------------------------------------
// Transfet inner class
// ----------------------------------------------------------------------------

  private class TransferImpl implements TransferPair {
    Nullable${minor.class}Vector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator){
      to = new Nullable${minor.class}Vector(field, allocator);
      <#if type.major == "VarLen" && minor.class == "VarChar">
      if (isDuplicateValsOnly()) {
        to.setDuplicateValsOnly(true);
    }
      </#if>
    }

    public TransferImpl(Nullable${minor.class}Vector to){
      this.to = to;
      <#if type.major == "VarLen" && minor.class == "VarChar">
      if (isDuplicateValsOnly()) {
        this.to.setDuplicateValsOnly(true);
    }
      </#if>
    }

    @Override
    public Nullable${minor.class}Vector getTo(){
      return to;
    }

    @Override
    public void transfer(){
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, Nullable${minor.class}Vector.this);
    }
  }

// ----------------------------------------------------------------------------
// Accessor inner classes
// ----------------------------------------------------------------------------

  /** Abstract mutator */
  public abstract class Accessor extends BaseDataValueVector.BaseAccessor <#if type.major = "VarLen">implements VariableWidthVector.VariableWidthAccessor</#if> {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();
    final ${valuesName}.Accessor vAccessor = values.getAccessor();

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  IllegalStateException if the value is null
     */
    abstract public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index);
    abstract public int isSet(int index);
    <#if type.major == "VarLen">
    abstract public long getStartEnd(int index);
    </#if>
    abstract public void get(int index, Nullable${minor.class}Holder holder);
    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    abstract public StringBuilder getAsStringBuilder(int index);
    </#if>
    public void reset() {}

    @Override
    abstract public ${friendlyType} getObject(int index);

  }

  /** Accessor Implementation */
  public final class AccessorImpl extends Accessor {
    /** {@inheritDoc} */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      if (isNull(index)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(index);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    /** {@inheritDoc} */
    public int isSet(int index){
      return bAccessor.get(index);
    }
    <#if type.major == "VarLen">
    /** {@inheritDoc} */
    public long getStartEnd(int index){
      return vAccessor.getStartEnd(index);
    }

    /** {@inheritDoc} */
    @Override
    public int getValueLength(int index) {
      return values.getAccessor().getValueLength(index);
    }
    </#if>
    /** {@inheritDoc} */
    public void get(int index, Nullable${minor.class}Holder holder){
      vAccessor.get(index, holder);
      holder.isSet = bAccessor.get(index);

      <#if minor.class.startsWith("Decimal")>
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
      </#if>
    }

    /** {@inheritDoc} */
    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(index);
      }
    }
    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    /** {@inheritDoc} */
    public StringBuilder getAsStringBuilder(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getAsStringBuilder(index);
      }
    }
    </#if>
    /** {@inheritDoc} */
    @Override
    public int getValueCount() {
      return bits.getAccessor().getValueCount();
    }
  }

  <#if type.major == "VarLen" && minor.class == "VarChar">
  /** Accessor Implementation for vector with only duplicate values */
  public final class DupValsOnlyAccessor extends Accessor {
    /** {@inheritDoc} */
    public byte[] get(int index) {
      chkIndex(index);

      if (isNull(0)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(0);
  }

    /** {@inheritDoc} */
    @Override
    public boolean isNull(int index) {
      chkIndex(index);
      return bAccessor.get(0) == 0;
    }

    /** {@inheritDoc} */
    public int isSet(int index) {
      chkIndex(index);
      return bAccessor.get(0);
    }

    /** {@inheritDoc} */
    public long getStartEnd(int index){
      chkIndex(index);
      return vAccessor.getStartEnd(0);
    }

    /** {@inheritDoc} */
    @Override
    public int getValueLength(int index) {
      chkIndex(index);
      return values.getAccessor().getValueLength(0);
    }

    /** {@inheritDoc} */
    public void get(int index, Nullable${minor.class}Holder holder) {
      chkIndex(index);
      vAccessor.get(0, holder);
      holder.isSet = bAccessor.get(0);
    }

    /** {@inheritDoc} */
    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(0);
      }
    }

    /** {@inheritDoc} */
    @Override
    public int getValueCount() {
      return logicalNumValues;
    }

    private void chkIndex(int index) {
      if (index >= logicalNumValues) {
        throw new IndexOutOfBoundsException(String.format("Index [%d], number of values [%d]", index, logicalNumValues));
      }
    }
  }
  </#if>

//-----------------------------------------------------------------------------
// Mutator inner classes
//-----------------------------------------------------------------------------

  /** Abstract mutator class */
  public abstract class Mutator extends BaseDataValueVector.BaseMutator implements NullableVectorDefinitionSetter<#if type.major = "VarLen">, VariableWidthVector.VariableWidthMutator</#if> {
    protected int setCount;
    <#if type.major = "VarLen">protected int lastSet = -1;</#if>

    private Mutator() { }

    abstract public ${valuesName} getVectorWithValues();
    /**
     * Set the variable length element at the specified index to the supplied value.
     *
     * @param index   position of the bit to set
     * @param value   value to write
     */
    abstract public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value);
    <#if type.major == "VarLen">
    abstract public void setSafe(int index, byte[] value, int start, int length);
    abstract public void setScalar(int index, byte[] value, int start, int length) throws VectorOverflowException;
    abstract public void setSafe(int index, ByteBuffer value, int start, int length);
    abstract public void setScalar(int index, DrillBuf value, int start, int length) throws VectorOverflowException;
    abstract protected void fillEmpties(int index);
    </#if>
    abstract public void setNull(int index);
    abstract public void setSkipNull(int index, ${minor.class}Holder holder);
    abstract public void setSkipNull(int index, Nullable${minor.class}Holder holder);
    abstract public void setNullBounded(int index) throws VectorOverflowException;
    abstract public void set(int index, Nullable${minor.class}Holder holder);
    abstract public void set(int index, ${minor.class}Holder holder);
    abstract public boolean isSafe(int outIndex);
    <#assign fields = minor.fields!type.fields />
    abstract public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> );
    abstract public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> );
    abstract public void setScalar(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) throws VectorOverflowException;
    abstract public void setSafe(int index, Nullable${minor.class}Holder value);
    abstract public void setScalar(int index, Nullable${minor.class}Holder value) throws VectorOverflowException;
    abstract public void setSafe(int index, ${minor.class}Holder value);
    abstract public void setScalar(int index, ${minor.class}Holder value) throws VectorOverflowException;
    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "Interval" || minor.class == "IntervalDay")>
    abstract public void setSafe(int index, ${minor.javaType!type.javaType} value);
    abstract public void setScalar(int index, ${minor.javaType!type.javaType} value) throws VectorOverflowException;
    </#if>
    <#if minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    abstract public void set(int index, BigDecimal value);
    abstract public void setSafe(int index, BigDecimal value);
    abstract public void setScalar(int index, BigDecimal value) throws VectorOverflowException;
    </#if>
    <#if type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    /**
     * Stores a set of bulk entries
     * @param input bulk input
     */
    abstract public void setSafe(VLBulkInput<VLBulkEntry> input);
    </#if>
    abstract public void fromNotNullable(${minor.class}Vector srce);
  }

  /** Mutator implementation class */
  public final class MutatorImpl extends Mutator {
     private MutatorImpl() { super(); }

    /** {@inheritDoc} */
    public ${valuesName} getVectorWithValues(){
      return values;
    }

    /** {@inheritDoc} */
    @Override
    public void setIndexDefined(int index){
      bits.getMutator().set(index, 1);
    }

    /** {@inheritDoc} */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setCount++;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      final UInt1Vector.Mutator bitsMutator = bits.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bitsMutator.set(index, 1);
      valuesMutator.set(index, value);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if type.major == "VarLen">
    protected void fillEmpties(int index) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      for (int i = lastSet; i < index; i++) {
        valuesMutator.setSafe(i + 1, emptyByteArray);
      }
      while(index > bits.getValueCapacity()) {
        bits.reAlloc();
      }
      lastSet = index;
    }

    /** {@inheritDoc} */
    @Override
    public void setValueLengthSafe(int index, int length) {
      values.getMutator().setValueLengthSafe(index, length);
      lastSet = index;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, byte[] value, int start, int length) {
       if (index > lastSet + 1) {
        fillEmpties(index);
      }

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      lastSet = index;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, byte[] value, int start, int length) throws VectorOverflowException {
      if (index > lastSet + 1) {
        fillEmpties(index); // Filling empties cannot overflow the vector
      }
      values.getMutator().setScalar(index, value, start, length);
      bits.getMutator().setSafe(index, 1);
      setCount++;
      lastSet = index;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ByteBuffer value, int start, int length) {
      if (index > lastSet + 1) {
        fillEmpties(index);
      }

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      lastSet = index;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, DrillBuf value, int start, int length) throws VectorOverflowException {
      if (index > lastSet + 1) {
        fillEmpties(index); // Filling empties cannot overflow the vector
      }

      values.getMutator().setScalar(index, value, start, length);
      bits.getMutator().setSafe(index, 1);
      setCount++;
      lastSet = index;
    }
    </#if>
    /** {@inheritDoc} */
    public void setNull(int index) {
      bits.getMutator().setSafe(index, 0);
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, ${minor.class}Holder holder) {
      values.getMutator().set(index, holder);
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, Nullable${minor.class}Holder holder) {
      values.getMutator().set(index, holder);
    }

    /** {@inheritDoc} */
    public void setNullBounded(int index) throws VectorOverflowException {
      bits.getMutator().setScalar(index, 0);
    }

    /** {@inheritDoc} */
    public void set(int index, Nullable${minor.class}Holder holder) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, holder.isSet);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void set(int index, ${minor.class}Holder holder) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, 1);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public boolean isSafe(int outIndex) {
      return outIndex < Nullable${minor.class}Vector.this.getValueCapacity();
    }

    <#assign fields = minor.fields!type.fields />
    /** {@inheritDoc} */
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, isSet);
      valuesMutator.set(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, isSet);
      values.getMutator().setSafe(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
   }

    /** {@inheritDoc} */
    public void setScalar(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) throws VectorOverflowException {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      values.getMutator().setScalar(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      bits.getMutator().setSafe(index, isSet);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setSafe(int index, Nullable${minor.class}Holder value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, value.isSet);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setScalar(int index, Nullable${minor.class}Holder value) throws VectorOverflowException {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      values.getMutator().setScalar(index, value);
      bits.getMutator().setSafe(index, value.isSet);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ${minor.class}Holder value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setScalar(int index, ${minor.class}Holder value) throws VectorOverflowException {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      values.getMutator().setScalar(index, value);
      bits.getMutator().setSafe(index, 1);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "Interval" || minor.class == "IntervalDay")>
    /** {@inheritDoc} */
    public void setSafe(int index, ${minor.javaType!type.javaType} value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, ${minor.javaType!type.javaType} value) throws VectorOverflowException {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      values.getMutator().setScalar(index, value);
      bits.getMutator().setSafe(index, 1);
      setCount++;
    }
    </#if>
    <#if minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    /** {@inheritDoc} */
    public void set(int index, BigDecimal value) {
      bits.getMutator().set(index, 1);
      values.getMutator().set(index, value);
      setCount++;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, BigDecimal value) {
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, BigDecimal value) throws VectorOverflowException {
      values.getMutator().setScalar(index, value);
      bits.getMutator().setSafe(index, 1);
      setCount++;
    }
    </#if>
    /** {@inheritDoc} */
    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      <#if type.major == "VarLen">
      fillEmpties(valueCount);
      </#if>
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }
    <#if type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    /** Enables this wrapper container class to participate in bulk mutator logic */
    private final class VLBulkInputCallbackImpl implements VLBulkInput.BulkInputCallback<VLBulkEntry> {
      /** The default buffer size */
      private static final int DEFAULT_BUFF_SZ = 1024 << 2;
      /** A buffered mutator to the bits vector */
      private final UInt1Vector.BufferedMutator bitsMutator;

      private VLBulkInputCallbackImpl(int _start_idx) {
        bitsMutator = new UInt1Vector.BufferedMutator(_start_idx, DEFAULT_BUFF_SZ, bits);
      }

      /** {@inheritDoc} */
    @Override
      public void onNewBulkEntry(final VLBulkEntry entry) {
        final int[] lengths       = entry.getValuesLength();
        final ByteBuffer buffer   = bitsMutator.getByteBuffer();
        final byte[] buffer_array = buffer.array();
        int remaining             = entry.getNumValues();
        int srcPos                = 0;

        // We need to set the bit indicators

        do {
          if (buffer.remaining() < 1) {
              bitsMutator.flush();
          }

          final int toCopy      = Math.min(remaining, buffer.remaining());
          final int startTgtPos = buffer.position();
          final int maxTgtPos   = startTgtPos + toCopy;

          if (entry.hasNulls()) {
            for (int idx = startTgtPos; idx < maxTgtPos; idx++) {
              final int valLen = lengths[srcPos++];

              if (valLen >= 0) {
                buffer_array[idx] = 1;
                ++setCount;
              } else {
                // This is a null entry
                buffer_array[idx] = 0;
              }
            }
          } else { // Optimization when there are no nulls within this bulk entry
            for (int idx = startTgtPos; idx < maxTgtPos; idx++) {
                buffer_array[idx] = 1;
            }
            setCount += toCopy;
          }

          // Update counters
          buffer.position(maxTgtPos);
          remaining -= toCopy;

        } while (remaining > 0);
        <#if type.major == "VarLen">
        // Update global counters
        lastSet += entry.getNumValues();
        </#if>
      }

      /** {@inheritDoc} */
      @Override
      public void onEndBulkInput() {
        bitsMutator.flush();
      }
    }

    /** {@inheritDoc} */
    public void setSafe(VLBulkInput<VLBulkEntry> input) {
      // Register a callback so that we can assign indicators to each value
      VLBulkInput.BulkInputCallback<VLBulkEntry> callback = new VLBulkInputCallbackImpl(input.getStartIndex());

      // Now delegate bulk processing to the value container
      values.getMutator().setSafe(input, callback);
    }
    </#if>
    /** {@inheritDoc} */
    @Override
    public void generateTestData(int valueCount){
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }

    /** {@inheritDoc} */
    @Override
    public void reset(){
      setCount = 0;
      <#if type.major = "VarLen">lastSet = -1;</#if>
    }

    // For nullable vectors, exchanging buffers (done elsewhere)
    // requires also exchanging mutator state (done here.)

    /** {@inheritDoc} */
    @Override
    public void exchange(ValueVector.Mutator other) {
      final Mutator target = (Mutator) other;
      int temp = setCount;
      setCount = target.setCount;
      target.setCount = temp;
    }

    /** TODO - document this method */
    @Override
    public void fromNotNullable(${minor.class}Vector srce) {
      clear();
      final int valueCount = srce.getAccessor().getValueCount();

      // Create a new bits vector, all values non-null

      fillBitsVector(getBitsVector(), valueCount);

      // Swap the data portion

      getValuesVector().exchange(srce);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }
  }

  <#if type.major == "VarLen" && minor.class == "VarChar">
  /** Duplicate values only class implementation */
  public final class DupValsOnlyMutator extends Mutator {
    private DupValsOnlyMutator() { super(); }

    /** {@inheritDoc} */
    public ${valuesName} getVectorWithValues(){
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void setIndexDefined(int index) {
      assert index == 0;
      bits.getMutator().set(0, 1);
    }

    /** {@inheritDoc} */
    public void set(int index, byte[] value) {
      assert index == 0;
      setCount = 1;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      final UInt1Vector.Mutator bitsMutator     = bits.getMutator();
      bitsMutator.set(0, 1);
      valuesMutator.set(0, value);
    }

    /** {@inheritDoc} */
    @Override
    public void setValueLengthSafe(int index, int length) {
      assert index == 0;
      values.getMutator().setValueLengthSafe(0, length);
    }

    /** {@inheritDoc} */
    public void setSafe(int index, byte[] value, int start, int length) {
      assert index == 0;
      bits.getMutator().setSafe(0, 1);
      values.getMutator().setSafe(0, value, start, length);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, byte[] value, int start, int length) throws VectorOverflowException {
      assert index == 0;
      values.getMutator().setScalar(0, value, start, length);
      bits.getMutator().setSafe(0, 1);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ByteBuffer value, int start, int length) {
      assert index == 0;
      bits.getMutator().setSafe(0, 1);
      values.getMutator().setSafe(0, value, start, length);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, DrillBuf value, int start, int length) throws VectorOverflowException {
      assert index == 0;
      values.getMutator().setScalar(0, value, start, length);
      bits.getMutator().setSafe(0, 1);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setNull(int index) {
      assert index == 0;
      bits.getMutator().setSafe(0, 0);
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, ${minor.class}Holder holder) {
      assert index == 0;
      values.getMutator().set(0, holder);
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, Nullable${minor.class}Holder holder) {
      assert index == 0;
      values.getMutator().set(0, holder);
    }

    /** {@inheritDoc} */
    public void setNullBounded(int index) throws VectorOverflowException {
      assert index == 0;
      bits.getMutator().setScalar(0, 0);
    }

    /** {@inheritDoc} */
    public void set(int index, Nullable${minor.class}Holder holder) {
      assert index == 0;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      bits.getMutator().set(0, holder.isSet);
      valuesMutator.set(0, holder);
    }

    /** {@inheritDoc} */
    public void set(int index, ${minor.class}Holder holder) {
      assert index == 0;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      bits.getMutator().set(0, 1);
      valuesMutator.set(0, holder);
    }

    /** {@inheritDoc} */
    public boolean isSafe(int outIndex) {
      throw new UnsupportedOperationException();
    }

    <#assign fields = minor.fields!type.fields />
    /** {@inheritDoc} */
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    public void setScalar(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) throws VectorOverflowException {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    public void setSafe(int index, Nullable${minor.class}Holder value) {
      assert index == 0;
      bits.getMutator().setSafe(0, value.isSet);
      values.getMutator().setSafe(0, value);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, Nullable${minor.class}Holder value) throws VectorOverflowException {
      assert index == 0;
      values.getMutator().setScalar(0, value);
      bits.getMutator().setSafe(0, value.isSet);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ${minor.class}Holder value) {
      assert index == 0;
      bits.getMutator().setSafe(0, 1);
      values.getMutator().setSafe(0, value);
      setCount = 1;
    }

    /** {@inheritDoc} */
    public void setScalar(int index, ${minor.class}Holder value) throws VectorOverflowException {
      assert index == 0;
      bits.getMutator().setSafe(0, 1);
      setCount = 1;
    }

    /** {@inheritDoc} */
    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      logicalNumValues     = valueCount;
      logicalValueCapacity = valueCount;

      values.getMutator().setValueCount(valueCount > 0 ? 1 : 0);
      bits.getMutator().setValueCount(valueCount > 0 ? 1 : 0);
    }

    /** {@inheritDoc} */
    public void setSafe(VLBulkInput<VLBulkEntry> input) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void generateTestData(int valueCount) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void reset(){
      setCount         = 0;
      logicalNumValues = 0;
    }

    // For nullable vectors, exchanging buffers (done elsewhere)
    // requires also exchanging mutator state (done here.)

    /** {@inheritDoc} */
    @Override
    public void exchange(ValueVector.Mutator other) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void fromNotNullable(${minor.class}Vector srce) {
      throw new UnsupportedOperationException();
    }

    protected void fillEmpties(int index) {}
  }
  </#if>
}
</#list>
</#list>
