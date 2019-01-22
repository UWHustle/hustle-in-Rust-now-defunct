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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_LOGICAL_LIMIT_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_LOGICAL_LIMIT_HPP_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/LogicalType.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace optimizer {
namespace logical {

/** \addtogroup OptimizerLogical
*  @{
*/

class Limit;
typedef std::shared_ptr<const Limit> LimitPtr;

/**
 * @brief Limit operator that reduces the size of the output.
 */
class Limit : public Logical {
 public:
  LogicalType getLogicalType() const override { return LogicalType::kLimit; }

  std::string getName() const override { return "Limit"; }

  /**
   * @return The input logical node.
   */
  const LogicalPtr &input() const { return input_; }

  std::vector<expressions::AttributeReferencePtr>
  getOutputAttributes() const override {
    return input()->getOutputAttributes();
  }

  std::vector<expressions::AttributeReferencePtr>
  getReferencedAttributes() const override {
    return input_->getReferencedAttributes();
  }

  /**
   * @return The number of output sorted rows. -1 for a full table sort.
   */
  int limit() const {
    return limit_;
  }

  /**
  * @brief Creates a Limit logical node.
  *
  * @param input The input to the Limit.
  * @param limit The number of output rows. -1 for a full table sort.
  *
  * @return An immutable Limit.
  */
  static LimitPtr Create(
      const LogicalPtr &input,
      const int limit) {
    return LimitPtr(new Limit(input,
                              limit));
  }

 private:
  Limit(const LogicalPtr &input, const int limit)
      : input_(input), limit_(limit) {}

  const LogicalPtr input_;
  const int limit_;
};

/** @} */

}  // namespace logical
}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_LOGICAL_LIMIT_HPP_ */
