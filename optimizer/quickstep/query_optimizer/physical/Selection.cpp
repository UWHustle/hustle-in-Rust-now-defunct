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

#include "query_optimizer/physical/Selection.hpp"

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/physical/PartitionSchemeHeader.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "utility/Cast.hpp"

#include "glog/logging.h"

using std::unordered_set;

namespace quickstep {
namespace optimizer {
namespace physical {

namespace E = ::quickstep::optimizer::expressions;

PhysicalPtr Selection::copyWithNewChildren(
    const std::vector<PhysicalPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0], project_expressions_, filter_predicate_,
                has_repartition_, cloneOutputPartitionSchemeHeader());
}

std::vector<E::AttributeReferencePtr> Selection::getOutputAttributes() const {
  return ToRefVector(project_expressions_);
}

std::vector<E::AttributeReferencePtr> Selection::getReferencedAttributes() const {
  std::vector<E::AttributeReferencePtr> referenced_attributes;
  for (const E::NamedExpressionPtr &project_expression : project_expressions_) {
    const std::vector<E::AttributeReferencePtr> referenced_attributes_in_expression =
        project_expression->getReferencedAttributes();
    referenced_attributes.insert(referenced_attributes.end(),
                                 referenced_attributes_in_expression.begin(),
                                 referenced_attributes_in_expression.end());
  }
  if (filter_predicate_ != nullptr) {
    const std::vector<E::AttributeReferencePtr> referenced_attributes_in_predicate =
        filter_predicate_->getReferencedAttributes();
    referenced_attributes.insert(referenced_attributes.end(),
                                 referenced_attributes_in_predicate.begin(),
                                 referenced_attributes_in_predicate.end());
  }
  return referenced_attributes;
}

PhysicalPtr Selection::copyWithNewProjectExpressions(
    const std::vector<E::NamedExpressionPtr> &output_expressions) const {
  DCHECK_EQ(project_expressions_.size(), output_expressions.size());

  return Create(input(), output_expressions, filter_predicate_);
}

bool Selection::maybeCopyWithPrunedExpressions(
    const E::UnorderedNamedExpressionSet &referenced_attributes,
    PhysicalPtr *output) const {
  std::vector<E::NamedExpressionPtr> new_project_expressions;
  for (const E::NamedExpressionPtr &project_expression : project_expressions_) {
    if (referenced_attributes.find(project_expression) !=
        referenced_attributes.end()) {
      new_project_expressions.emplace_back(project_expression);
    }
  }
  if (new_project_expressions.size() != project_expressions_.size()) {
    *output = Create(input(), new_project_expressions, filter_predicate_,
                     has_repartition_, cloneOutputPartitionSchemeHeader());
    return true;
  }
  return false;
}

void Selection::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  inline_field_names->push_back("has_repartition");
  inline_field_values->push_back(has_repartition_ ? "true" : "false");

  if (partition_scheme_header_) {
    inline_field_names->push_back("output_partition_scheme_header");
    inline_field_values->push_back(partition_scheme_header_->toString());
  }

  non_container_child_field_names->emplace_back("input");
  non_container_child_fields->emplace_back(input());

  if (filter_predicate_ != nullptr) {
    non_container_child_field_names->emplace_back("filter_predicate");
    non_container_child_fields->emplace_back(filter_predicate_);
  }

  container_child_field_names->emplace_back("project_expressions");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(project_expressions_));
}

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep
