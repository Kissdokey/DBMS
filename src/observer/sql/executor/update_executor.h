#pragma once
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
#include "common/rc.h"
class SQLStageEvent;
class RecordFileScanner;
/**
 * @brief 创建表的执行器
 * @ingroup Executor
 */
class UpdateExecutor
{
public:
  UpdateExecutor() = default;
  virtual ~UpdateExecutor() = default;
  bool ifFilterFit(AttrType type,const char * value,const char * filter_value,CompOp op);
  RC execute(SQLStageEvent *sql_event);
};