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

#include "drill/common.hpp"
#include "drill/fieldmeta.hpp"
#include "../protobuf/UserBitShared.pb.h"
#include "../protobuf/User.pb.h"

namespace {
static std::string getSQLType(common::MinorType type, common::DataMode mode) {
  if (mode == common::DM_REPEATED || type == common::LIST) {
    return "ARRAY";
  }

  switch(type) {
    case common::BIT:             return "BOOLEAN";

    case common::SMALLINT:        return "SMALLINT";
    case common::INT:             return "INTEGER";
    case common::BIGINT:          return "BIGINT";
    case common::FLOAT4:          return "FLOAT";
    case common::FLOAT8:          return "DOUBLE";

    case common::DECIMAL9:
    case common::DECIMAL18:
    case common::DECIMAL28DENSE:
    case common::DECIMAL38DENSE:
    case common::DECIMAL38SPARSE: return "DECIMAL";

    case common::VARCHAR:         return "CHARACTER VARYING";
    case common::FIXEDCHAR:       return "CHARACTER";

    case common::VAR16CHAR:       return "NATIONAL CHARACTER VARYING";
    case common::FIXED16CHAR:     return "NATIONAL CHARACTER";

    case common::VARBINARY:       return "BINARY VARYING";
    case common::FIXEDBINARY:     return "BINARY";

    case common::DATE:            return "DATE";
    case common::TIME:            return "TIME";
    case common::TIMETZ:          return "TIME WITH TIME ZONE";
    case common::TIMESTAMP:       return "TIMESTAMP";
    case common::TIMESTAMPTZ:     return "TIMESTAMP WITH TIME ZONE";

    case common::INTERVALYEAR:
    case common::INTERVALDAY:
    case common::INTERVAL:        return "INTERVAL";
    case common::MONEY:           return "DECIMAL";
    case common::TINYINT:         return "TINYINT";

    case common::MAP:             return "MAP";
    case common::LATE:            return "ANY";
    case common::DM_UNKNOWN:      return "NULL";
    case common::UNION:           return "UNION";

    case common::UINT1:           return "TINYINT";
    case common::UINT2:           return "SMALLINT";
    case common::UINT4:           return "INTEGER";
    case common::UINT8:           return "BIGINT";

    default:
      return "__unknown__";
  }
}

static bool isSortable(common::MinorType type) {
  return type != common::MAP && type != common::LIST;
}

static bool isNullable(common::DataMode mode) {
  return mode == common::DM_OPTIONAL; // Same behaviour as JDBC
}

static bool isSigned(common::MinorType type, common::DataMode mode) {
  if (mode == common::DM_REPEATED) {
    return false;// SQL ARRAY
  }

  switch(type) {
    case common::SMALLINT:
    case common::INT:
    case common::BIGINT:
    case common::FLOAT4:
    case common::FLOAT8:

    case common::DECIMAL9:
    case common::DECIMAL18:
    case common::DECIMAL28DENSE:
    case common::DECIMAL38DENSE:
    case common::DECIMAL38SPARSE:

    case common::INTERVALYEAR:
    case common::INTERVALDAY:
    case common::INTERVAL:
    case common::MONEY:
    case common::TINYINT:
      return true;

    case common::BIT:
    case common::VARCHAR:
    case common::FIXEDCHAR:

    case common::VAR16CHAR:
    case common::FIXED16CHAR:

    case common::VARBINARY:
    case common::FIXEDBINARY:

    case common::DATE:
    case common::TIME:
    case common::TIMETZ:
    case common::TIMESTAMP:
    case common::TIMESTAMPTZ:

    case common::MAP:
    case common::LATE:
    case common::DM_UNKNOWN:
    case common::UNION:

    case common::UINT1:
    case common::UINT2:
    case common::UINT4:
    case common::UINT8:
      return false;

    default:
      return false;
  }
}

static Drill::FieldMetadata::ColumnSearchability getSearchability(exec::user::ColumnSearchability s) {
  switch(s) {
    case exec::user::UNKNOWN_SEARCHABILITY: return Drill::FieldMetadata::UNKNOWN_SEARCHABILITY;
    case exec::user::NONE:                  return Drill::FieldMetadata::NONE;
    case exec::user::CHAR:                  return Drill::FieldMetadata::CHAR;
    case exec::user::NUMBER:                return Drill::FieldMetadata::NUMBER;
    case exec::user::ALL:                   return Drill::FieldMetadata::ALL;

    default:
      return Drill::FieldMetadata::UNKNOWN_SEARCHABILITY;
  }
}

static Drill::FieldMetadata::ColumnUpdatability getUpdatability(exec::user::ColumnUpdatability u) {
  switch(u) {
    case exec::user::UNKNOWN_UPDATABILITY: return Drill::FieldMetadata::UNKNOWN_UPDATABILITY;
    case exec::user::READ_ONLY:            return Drill::FieldMetadata::READ_ONLY;
    case exec::user::WRITABLE:             return Drill::FieldMetadata::WRITABLE;

    default:
      return Drill::FieldMetadata::UNKNOWN_UPDATABILITY;
  }
}
} // namespace

namespace Drill{

void FieldMetadata::set(const exec::shared::SerializedField& f){
    m_name=f.name_part().name();
    m_minorType=f.major_type().minor_type();
    m_dataMode=f.major_type().mode();
    m_valueCount=f.value_count();
    m_scale=f.major_type().scale();
    m_precision=f.major_type().precision();
    m_bufferLength=f.buffer_length();
    m_catalogName="DRILL";
    m_schemaName=""; // unknown
    m_tableName=""; // unknown;
    m_label=m_name;
    m_sqlType=::getSQLType(m_minorType, m_dataMode);
    m_nullable=::isNullable(m_dataMode);
    m_displaySize=10; // same as JDBC
    m_signed=::isSigned(m_minorType, m_dataMode);
    m_searchability=ALL;
    m_updatability=READ_ONLY;
    m_autoIncremented=false;
    m_caseSensitive=false;
    m_sortable=::isSortable(m_minorType);
    m_currency=false;
}

void FieldMetadata::set(const exec::user::ResultColumnMetadata& m){
    m_name=m.column_name();
    m_minorType=static_cast<common::MinorType>(-1);
    m_dataMode=static_cast<common::DataMode>(-1);
    m_valueCount=0;
    m_scale=m.scale();
    m_precision=m.precision();
    m_bufferLength=0;
    m_catalogName=m.catalog_name();
    m_schemaName=m.schema_name();
    m_tableName=m.table_name();
    m_label=m.label();
    m_sqlType=m.data_type();
    m_nullable=m.is_nullable();
    m_displaySize=m.display_size();
    m_signed=m.signed_();
    m_searchability=::getSearchability(m.searchability());
    m_updatability=::getUpdatability(m.updatability());
    m_autoIncremented=m.auto_increment();
    m_caseSensitive=m.case_sensitivity();
    m_sortable=m.sortable();
    m_currency=m.is_currency();
}

}// namespace Drill

