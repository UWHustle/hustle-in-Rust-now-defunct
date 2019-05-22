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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIMIT_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIMIT_HPP_

#include <memory>
#include <string>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {
namespace physical {

/** \addtogroup OptimizerPhysical
 *  @{
 */

class Limit;

typedef std::shared_ptr<const Limit> LimitPtr;

/**
 * @brief Limit operator.
 *
 * Returns the first limit records of the input relation.
 */
class Limit : public Physical {
 public:
  PhysicalType getPhysicalType() const override {
    return PhysicalType::kLimit;
  }

  std::string getName() const override {
    return "Limit";
  }

  const PhysicalPtr &input() const {
    return input_;
  }

  std::vector<expressions::AttributeReferencePtr>
  getOutputAttributes() const override {
    return input_->getOutputAttributes();
  }

  std::vector<expressions::AttributeReferencePtr>
  getReferencedAttributes() const override {
    return input_->getReferencedAttributes();
  }

  bool maybeCopyWithPrunedExpressions(
      const expressions::UnorderedNamedExpressionSet &referenced_expressions,
      PhysicalPtr *output) const override {
    return false;
  }

  PhysicalPtr copyWithNewChildren(
      const std::vector<PhysicalPtr> &new_children) const override {
    DCHECK_EQ(children().size(), new_children.size());
    return Create(new_children[0], limit_);
  }

  /**
* @return The number of output rows.
*/
  int limit() const {
    return limit_;
  }

  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override {
    non_container_child_field_names->emplace_back("input");
    non_container_child_fields->emplace_back(input());

    inline_field_names->emplace_back("limit");
    inline_field_values->emplace_back(std::to_string(limit_));
  }

  /**
* @brief Creates a physical Limit operator.
*
* @param input The input to the Limit.
* @param limit The number of output rows.
*
* @return An immutable physical Limit.
*/
  static LimitPtr Create(const PhysicalPtr &input,
                        const int limit) {
    DCHECK_GE(limit, 0);
    return LimitPtr(new Limit(input, limit));
  }

 private:
  Limit(const PhysicalPtr &input,
        const int limit) : input_(input), limit_(limit) {}

  const PhysicalPtr input_;
  const int limit_;

  DISALLOW_COPY_AND_ASSIGN(Limit);
};

/** @} */

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIMIT_HPP_ */
