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

typedef std::shared_ptr<const Sort> LimitPtr;

/**
 * @brief Limit operator.
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

  /**
* @return The number of output sorted rows.
*/
  int limit() const {
    return limit_;
  }

  /**
* @brief Creates a physical Sort operator.
*
* @param input The input to the Limit.
* @param limit The number of output rows. -1 for a full table sort.
*
* @return An immutable physical Limit.
*/
  static SortPtr Create(const PhysicalPtr &input,
                        const int limit) {
    DCHECK_GE(limit, 0);

    return SortPtr(new Sort(input,
                            limit));
  }

 private:
  Sort(const PhysicalPtr &input,
       const int limit) : input_(input),

                          limit_(limit) {}

  const PhysicalPtr input_;
  const int limit_;

  DISALLOW_COPY_AND_ASSIGN(Sort);
};

/** @} */

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIMIT_HPP_ */
