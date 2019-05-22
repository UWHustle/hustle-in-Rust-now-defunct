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

#include "utility/PlanVisualizer.hpp"

#include <cstddef>
#include <exception>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/physical/FilterJoin.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/PartitionSchemeHeader.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "query_optimizer/physical/TableReference.hpp"
#include "query_optimizer/physical/TopLevelPlan.hpp"
#include "utility/StringUtil.hpp"

#include "glog/logging.h"

namespace quickstep {

namespace E = ::quickstep::optimizer::expressions;
namespace P = ::quickstep::optimizer::physical;
namespace C = ::quickstep::optimizer::cost;

std::string PlanVisualizer::visualize(const P::PhysicalPtr &input) {
  DCHECK(input->getPhysicalType() == P::PhysicalType::kTopLevelPlan);
  const P::TopLevelPlanPtr top_level_plan =
      std::static_pointer_cast<const P::TopLevelPlan>(input);
  cost_model_.reset(
      new C::StarSchemaSimpleCostModel(
          top_level_plan->shared_subplans()));
  lip_filter_conf_ = top_level_plan->lip_filter_configuration();

  color_map_["TableReference"] = "skyblue";
  color_map_["Selection"] = "#90EE90";
  color_map_["FilterJoin"] = "pink";
  color_map_["FilterJoin(Anti)"] = "pink";
  color_map_["HashJoin"] = "red";
  color_map_["HashLeftOuterJoin"] = "orange";
  color_map_["HashLeftSemiJoin"] = "orange";
  color_map_["HashLeftAntiJoin"] = "orange";

  visit(input);

  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=16 fontcolor=gray penwidth=2]\n\n";

  // Format nodes
  for (const NodeInfo &node_info : nodes_) {
    graph_oss << "  " << node_info.id << " [";
    if (!node_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(JoinToString(node_info.labels, "&#10;"))
                << "\"";
    }
    if (!node_info.color.empty()) {
      graph_oss << " style=filled fillcolor=\"" << node_info.color << "\"";
    }
    graph_oss << "]\n";
  }
  graph_oss << "\n";

  // Format edges
  for (const EdgeInfo &edge_info : edges_) {
    graph_oss << "  " << edge_info.src_node_id << " -> "
              << edge_info.dst_node_id << " [";
    if (edge_info.dashed) {
      graph_oss << "style=dashed ";
    }
    if (!edge_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(JoinToString(edge_info.labels, "&#10;"))
                << "\"";
    }
    graph_oss << "]\n";
  }

  graph_oss << "}\n";

  return graph_oss.str();
}

void PlanVisualizer::visit(const P::PhysicalPtr &input) {
  int node_id = ++id_counter_;
  node_id_map_.emplace(input, node_id);

  std::set<E::ExprId> referenced_ids;
  for (const auto &attr : input->getReferencedAttributes()) {
    referenced_ids.emplace(attr->id());
  }
  for (const auto &child : input->children()) {
    visit(child);

    int child_id = node_id_map_[child];

    edges_.emplace_back(EdgeInfo());
    EdgeInfo &edge_info = edges_.back();
    edge_info.src_node_id = child_id;
    edge_info.dst_node_id = node_id;
    edge_info.dashed = false;

    if ((input->getPhysicalType() == P::PhysicalType::kHashJoin ||
         input->getPhysicalType() == P::PhysicalType::kFilterJoin) &&
        child == input->children()[1]) {
      edge_info.dashed = true;
    }

    for (const auto &attr : child->getOutputAttributes()) {
      if (referenced_ids.find(attr->id()) != referenced_ids.end()) {
        edge_info.labels.emplace_back(attr->attribute_alias());
      }
    }
  }

  nodes_.emplace_back(NodeInfo());
  NodeInfo &node_info = nodes_.back();
  node_info.id = node_id;
  if (color_map_.find(input->getName()) != color_map_.end()) {
    node_info.color = color_map_[input->getName()];
  }

  switch (input->getPhysicalType()) {
    case P::PhysicalType::kTableReference: {
      const P::TableReferencePtr table_reference =
        std::static_pointer_cast<const P::TableReference>(input);
      node_info.labels.emplace_back(table_reference->relation()->getName());
      break;
    }
    case P::PhysicalType::kHashJoin: {
      const P::HashJoinPtr hash_join =
        std::static_pointer_cast<const P::HashJoin>(input);
      node_info.labels.emplace_back(input->getName());

      const auto &left_attributes = hash_join->left_join_attributes();
      const auto &right_attributes = hash_join->right_join_attributes();
      for (std::size_t i = 0; i < left_attributes.size(); ++i) {
        node_info.labels.emplace_back(
            left_attributes[i]->attribute_alias() + " = " + right_attributes[i]->attribute_alias());
      }
      break;
    }
    case P::PhysicalType::kFilterJoin: {
      const P::FilterJoinPtr filter_join =
          std::static_pointer_cast<const P::FilterJoin>(input);
      node_info.labels.emplace_back(input->getName());

      const auto &probe_attributes = filter_join->probe_attributes();
      const auto &build_attributes = filter_join->build_attributes();
      for (std::size_t i = 0; i < probe_attributes.size(); ++i) {
        node_info.labels.emplace_back(
            probe_attributes[i]->attribute_alias() + " = " +
                build_attributes[i]->attribute_alias());
      }
      break;
    }
    default: {
      node_info.labels.emplace_back(input->getName());
      break;
    }
  }

  const P::PartitionSchemeHeader *partition_scheme_header = input->getOutputPartitionSchemeHeader();
  if (partition_scheme_header) {
    node_info.labels.emplace_back(partition_scheme_header->toString());
  }

  if (lip_filter_conf_ != nullptr) {
    const auto &build_filters = lip_filter_conf_->getBuildInfoMap();
    const auto build_it = build_filters.find(input);
    if (build_it != build_filters.end()) {
      for (const auto &build_info : build_it->second) {
        node_info.labels.emplace_back(
            std::string("[LIP build] ") + build_info->build_attribute()->attribute_alias());
      }
    }
    const auto &probe_filters = lip_filter_conf_->getProbeInfoMap();
    const auto probe_it = probe_filters.find(input);
    if (probe_it != probe_filters.end()) {
      for (const auto &probe_info : probe_it->second) {
        node_info.labels.emplace_back(
            std::string("[LIP probe] ") + probe_info->probe_attribute()->attribute_alias());
      }
    }
  }

  try {
    const std::size_t estimated_cardinality = cost_model_->estimateCardinality(input);
    const double estimated_selectivity = cost_model_->estimateSelectivity(input);

    node_info.labels.emplace_back(
        "est. # = " + std::to_string(estimated_cardinality));
    node_info.labels.emplace_back(
        "est. Selectivity = " + std::to_string(estimated_selectivity));
  } catch (const std::exception &e) {
    // NOTE(jianqiao): CostModel::estimateCardinality() may throw UnsupportedPhysicalPlan
    // exception for some type of physical nodes such as CreateTable.
    // In this case, we omit the node's cardinality/selectivity information.
  }
}

}  // namespace quickstep
