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
// Created by Meiyi
//

#include <mutex>
#include "sql/parser/parse.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"

RC parse(char *st, ParsedSqlNode *sqln);

CalcSqlNode::~CalcSqlNode()
{
  for (Expression *expr : expressions) {
    delete expr;
  }
  expressions.clear();
}

ParsedSqlNode::ParsedSqlNode() : flag(SCF_ERROR)
{}

ParsedSqlNode::ParsedSqlNode(SqlCommandFlag _flag) : flag(_flag)
{}

void ParsedSqlResult::add_sql_node(std::unique_ptr<ParsedSqlNode> sql_node)
{
  sql_nodes_.emplace_back(std::move(sql_node));
}

////////////////////////////////////////////////////////////////////////////////

int sql_parse(const char *st, ParsedSqlResult *sql_result);
bool check_date(int y, int m, int d)
{
  // TODO 根据 y:year,m:month,d:day 校验日期是否合法
  // TODO 合法 return 1
  // TODO 不合法 return 0
  if (y < 1970 || y > 2038) return 0;
  if (m < 1 || m > 12) return 0;
  int mx_day; // mx_day记录当月最大天数
  if (m == 2) {
      if (y % 4 == 0 && y % 100 != 0 || y % 400 == 0) mx_day = 29; // 闰年
      else mx_day = 28;
  } else if (m <= 7) {
      if (m % 2 == 1) mx_day = 31;
      else mx_day = 30;
  } else if (m % 2 == 1) mx_day = 30;
  else mx_day = 31;
  if (d > mx_day) return 0;
  // TODO more judgement
  return 1;
}
int value_init_date(Value *value, const char *v) {
  // TODO 将 value 的 type 属性修改为日期属性:DATES
  value->set_type(DATES) ;
  // 从lex的解析中读取 year,month,day
  int y,m,d;
  sscanf(v, "%d-%d-%d", &y, &m, &d);//not check return value eq 3, lex guarantee
  // 对读取的日期做合法性校验
  bool b = check_date(y,m,d);
  if(!b) return -1;
  // TODO 将日期转换成整数
  int date_value = y * 400 + m * 35 + d;
  // TODO 将value 的 data 属性修改为转换后的日期
  value->set_data((char *)malloc(sizeof(int)),1);
  memcpy((void *)(value->data()), &date_value, sizeof(int));
  return 0;
}
RC parse(const char *st, ParsedSqlResult *sql_result)
{
  sql_parse(st, sql_result);
  return RC::SUCCESS;
}
