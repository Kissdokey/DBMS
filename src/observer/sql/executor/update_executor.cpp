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
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/executor/update_executor.h"

#include "session/session.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "sql/stmt/update_stmt.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "storage/db/db.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/delete_physical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/operator/table_scan_physical_operator.h"
#include "sql/stmt/filter_stmt.h"
RC UpdateExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  Trx     *trx     = session->current_trx();
  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }
  // 获取当前表格数据，遍历，符合条件的（fitler），进行修改数据
  UpdateStmt               *update_stmt    = (UpdateStmt *)stmt;
  std::vector<FilterUnit *> filter_units   = update_stmt->filter_stmt()->filter_units();  // 只有一个条件
  Value                    *values         = update_stmt->values();
  std::string               attribute_name = (*filter_units.begin())->left().field.field_name();
  const char               *b              = (*filter_units.begin())->right().value.data();
  // 获取到关键信息，参考delete和insert，看看如何操作表数据

  std::cout << update_stmt->attribute_name() << values->data() << attribute_name << b << endl;

  RecordFileScanner scanner;
  RC                rc = update_stmt->table()->get_record_scanner(scanner, trx, false /*readonly*/);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index." );
    return rc;
  }
  //Todo: 每个字段数据根据op进行比较，比较方式根据什么比较呢？需不需要case判断类型；左右数据类型判断，是字段还是值；
  Record record;
  while (scanner.has_next()) {
    rc = scanner.next(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to scan records while creating index.");
      return rc;
    }
    // 判断record是否符合条件，符合条件则调用table的update-record进行更新
    const TableMeta table_meta               = update_stmt->table()->table_meta();
    const int       normal_field_start_index = table_meta.sys_field_num();
    for (int i = normal_field_start_index; i < table_meta.field_num(); i++) {
      const FieldMeta *field = table_meta.field(i);
      if (0 != strcmp(field->name(), attribute_name.c_str())) {
        continue;
      }
      // 找到了字段，根据record获取数据进行判断
      // if (field->type() != values->attr_type()) {
      //   LOG_ERROR("Invalid value type. table name =%s, field name=%s, type=%d, but given=%d",
      //     table_meta.name(),
      //     field->name(),
      //     field->type(),
      //     values->attr_type());
      //   return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      // }
      const char *table_value = (const char *)(record.data() + field->offset());
      if (0 == strcmp(table_value, b)) {
        std::cout << record.data() << endl;
        rc = update_stmt->table()->update_record(trx, &record, update_stmt->attribute_name().c_str(), values);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to insert record into index while creating index.");
          return rc;
        }
      }
    }
  }
    scanner.close_scan();

    std::cout << update_stmt->values()->attr_type() << update_stmt->table();
    rc = RC::SUCCESS;
    return rc;
  }
