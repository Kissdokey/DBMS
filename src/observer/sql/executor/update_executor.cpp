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
RC UpdateExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }
  UpdateStmt *update_stmt = (UpdateStmt *)stmt;
  std::cout<<update_stmt->values()->attr_type()<<update_stmt->table();
  RC rc ;
  return rc;
}
