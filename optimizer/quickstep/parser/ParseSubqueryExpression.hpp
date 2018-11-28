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
 **/

#ifndef QUICKSTEP_PARSER_PARSE_SUBQUERY_EXPRESSION_HPP_
#define QUICKSTEP_PARSER_PARSE_SUBQUERY_EXPRESSION_HPP_

#include <memory>
#include <string>
#include <vector>

#include "parser/ParseExpression.hpp"
#include "parser/ParseSetOperation.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class ParseTreeNode;

/** \addtogroup Parser
 *  @{
 */

/**
 * @brief Parsed representation of a subquery expression.
 */
class ParseSubqueryExpression : public ParseExpression {
 public:
  /**
   * @brief Constructor. Takes ownership of \p query.
   *
   * @param line_number The line number of the beginning of the subquery expression.
   * @param column_number The column number of the beginning of the subquery expression.
   * @param set_operation The set operation subquery.
   */
  ParseSubqueryExpression(const int line_number,
                          const int column_number,
                          ParseSetOperation *set_operation)
      : ParseExpression(line_number, column_number),
        set_operation_(set_operation) {
  }

  ~ParseSubqueryExpression() override {
  }

  ExpressionType getExpressionType() const override {
    return kSubqueryExpression;
  }

  /**
   * @return The set operation subquery.
   */
  const ParseSetOperation* set_operation() const { return set_operation_.get(); }

  std::string getName() const override { return "SubqueryExpression"; }

  std::string generateName() const override;

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<const ParseTreeNode*> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<const ParseTreeNode*>> *container_child_fields) const override;

 private:
  std::unique_ptr<ParseSetOperation> set_operation_;
  DISALLOW_COPY_AND_ASSIGN(ParseSubqueryExpression);
};

/** @} */

}  // namespace quickstep

#endif /* QUICKSTEP_PARSER_PARSE_SUBQUERY_EXPRESSION_HPP_ */
