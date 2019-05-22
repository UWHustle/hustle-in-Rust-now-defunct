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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_FILTER_JOIN_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_FILTER_JOIN_HPP_

#include <memory>
#include <string>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/physical/BinaryJoin.hpp"
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

class FilterJoin;
typedef std::shared_ptr<const FilterJoin> FilterJoinPtr;

struct PartitionSchemeHeader;

/**
 * @brief Physical filter join node. Semantically, FilterJoin is similar to
 *        HashJoin where the difference is that FilterJoin builds a bit vector
 *        instead of a hash table.
 *
 * @note FilterJoin's backend execution relies on LIPFilter injection (attach
 *       the bit vectors as filters into downstream relational operators).
 */
class FilterJoin : public BinaryJoin {
 public:
  PhysicalType getPhysicalType() const override {
    return PhysicalType::kFilterJoin;
  }

  std::string getName() const override {
    if (is_anti_join_) {
      return "FilterJoin(Anti)";
    } else {
      return "FilterJoin";
    }
  }

  /**
   * @return The probe side attributes.
   */
  const std::vector<expressions::AttributeReferencePtr>& probe_attributes() const {
    return probe_attributes_;
  }

  /**
   * @return The build side attributes.
   */
  const std::vector<expressions::AttributeReferencePtr>& build_attributes() const {
    return build_attributes_;
  }

  /**
   * @return The build side filter predicate.
   */
  const expressions::PredicatePtr& build_side_filter_predicate() const {
    return build_side_filter_predicate_;
  }

  /**
   * @return Whether this is an anti-join.
   */
  const bool is_anti_join() const {
    return is_anti_join_;
  }

  PhysicalPtr copyWithNewChildren(
      const std::vector<PhysicalPtr> &new_children) const override {
    DCHECK_EQ(children().size(), new_children.size());
    return Create(new_children[0],
                  new_children[1],
                  probe_attributes_,
                  build_attributes_,
                  project_expressions(),
                  build_side_filter_predicate_,
                  is_anti_join_,
                  has_repartition_,
                  cloneOutputPartitionSchemeHeader());
  }

  std::vector<expressions::AttributeReferencePtr> getReferencedAttributes() const override;

  bool maybeCopyWithPrunedExpressions(
      const expressions::UnorderedNamedExpressionSet &referenced_expressions,
      PhysicalPtr *output) const override;

  /**
   * @brief Creates a physical FilterJoin.
   * @param probe_child The probe side child plan.
   * @param build_child The build side child plan.
   * @param probe_attributes The probe side attributes.
   * @param build_attributes The build side attributes.
   * @param project_expressions The project expressions.
   * @param build_side_filter_predicate Optional filtering predicate to be
   *        applied to the build side child BEFORE join.
   * @param is_anti_join Whether this is an anti-join.
   * @param has_repartition Whether this node does repartition.
   * @param partition_scheme_header The optional output partition scheme header.
   *
   * @return An immutable physical FilterJoin.
   */
  static FilterJoinPtr Create(
      const PhysicalPtr &probe_child,
      const PhysicalPtr &build_child,
      const std::vector<expressions::AttributeReferencePtr> &probe_attributes,
      const std::vector<expressions::AttributeReferencePtr> &build_attributes,
      const std::vector<expressions::NamedExpressionPtr> &project_expressions,
      const expressions::PredicatePtr &build_side_filter_predicate,
      const bool is_anti_join,
      const bool has_repartition = false,
      PartitionSchemeHeader *partition_scheme_header = nullptr) {
    return FilterJoinPtr(
        new FilterJoin(probe_child,
                       build_child,
                       probe_attributes,
                       build_attributes,
                       project_expressions,
                       build_side_filter_predicate,
                       is_anti_join,
                       has_repartition,
                       partition_scheme_header));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override;

 private:
  FilterJoin(
      const PhysicalPtr &probe_child,
      const PhysicalPtr &build_child,
      const std::vector<expressions::AttributeReferencePtr> &probe_attributes,
      const std::vector<expressions::AttributeReferencePtr> &build_attributes,
      const std::vector<expressions::NamedExpressionPtr> &project_expressions,
      const expressions::PredicatePtr &build_side_filter_predicate,
      const bool is_anti_join,
      const bool has_repartition,
      PartitionSchemeHeader *partition_scheme_header)
      : BinaryJoin(probe_child, build_child, project_expressions,
                   has_repartition, partition_scheme_header),
        probe_attributes_(probe_attributes),
        build_attributes_(build_attributes),
        build_side_filter_predicate_(build_side_filter_predicate),
        is_anti_join_(is_anti_join) {
  }

  const std::vector<expressions::AttributeReferencePtr> probe_attributes_;
  const std::vector<expressions::AttributeReferencePtr> build_attributes_;
  const expressions::PredicatePtr build_side_filter_predicate_;
  const bool is_anti_join_;

  DISALLOW_COPY_AND_ASSIGN(FilterJoin);
};

/** @} */

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_FILTER_JOIN_HPP_
