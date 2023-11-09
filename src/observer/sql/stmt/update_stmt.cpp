/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//
#include "common/log/log.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/update_stmt.h"

UpdateStmt::UpdateStmt(Table *table, Value *values,std::string attribute_name, int value_amount, FilterStmt *filter_stmt)
  : table_ (table), values_(values),attribute_name_(attribute_name), value_amount_(value_amount), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  // TODO
   if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  } 
}
RC UpdateStmt::create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt)
{
  const char *table_name = update_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db = %s, table = %s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields exist
  const TableMeta &table_meta = table->table_meta();
  const FieldMeta *field_meta = table_meta.field(update_sql.attribute_name.c_str());
  if (nullptr == field_meta) {
      LOG_WARN("no such field. table=%s, field=%s", table_name, update_sql.attribute_name);
      return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // check the field type
  if (field_meta->type() != update_sql.value.attr_type()) {
      LOG_WARN("field type not match. field=%s, field_type=%d, value_type=%d",
               field_meta->name(), field_meta->type(), update_sql.value.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map,
                     &(*update_sql.conditions.begin()), update_sql.conditions.size(), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

    // 遍历当前的所有数据，插入这个索引
  Value *values = const_cast<Value*>(&update_sql.value);
  stmt = new UpdateStmt(table,values,update_sql.attribute_name,1, filter_stmt);

  return RC::SUCCESS;
}
