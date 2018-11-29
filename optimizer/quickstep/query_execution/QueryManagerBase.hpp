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

#ifndef QUICKSTEP_QUERY_EXECUTION_QUERY_MANAGER_BASE_HPP_
#define QUICKSTEP_QUERY_EXECUTION_QUERY_MANAGER_BASE_HPP_

#include <cstddef>
#include <memory>
#include <unordered_set>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "utility/DAG.hpp"
#include "utility/ExecutionDAGVisualizer.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A base class that manages the execution of a query including
 *        generation of new work orders, and keeping track of the query
 *        exection state.
 **/
class QueryManagerBase {
 public:
  typedef DAG<RelationalOperator, bool>::size_type_nodes dag_node_index;

  /**
   * @brief Return codes for queryStatus() function.
   *
   * @note When both operator and query get executed, kQueryExecuted takes
   *       precedence over kOperatorExecuted.
   **/
  enum class QueryStatusCode {
    kOperatorExecuted = 0,  // An operator in the query finished execution.
    kQueryExecuted,         // The query got executed.
    kNone                   // None of the above.
  };

  /**
   * @brief Constructor.
   *
   * @param query_handle The QueryHandle object for this query.
   **/
  explicit QueryManagerBase(QueryHandle *query_handle);

  /**
   * @brief Virtual destructor.
   **/
  virtual ~QueryManagerBase() {}

  /**
   * @brief Get the query handle.
   **/
  const QueryHandle* query_handle() const {
    return query_handle_.get();
  }

  /**
   * @brief Get the QueryExecutionState for this query.
   **/
  inline const QueryExecutionState& getQueryExecutionState() const {
    return *query_exec_state_;
  }

  /**
   * @brief Process the received WorkOrder complete message.
   *
   * @param op_index The index of the specified operator node in the query DAG
   *        for the completed WorkOrder.
   * @param part_id The partition id.
   **/
  void processWorkOrderCompleteMessage(const dag_node_index op_index,
                                       const partition_id part_id);

  /**
   * @brief Process the received RebuildWorkOrder complete message.
   *
   * @param op_index The index of the specified operator node in the query DAG
   *        for the completed RebuildWorkOrder.
   * @param part_id The partition id.
   **/
  void processRebuildWorkOrderCompleteMessage(const dag_node_index op_index,
                                              const partition_id part_id);

  /**
   * @brief Process the received data pipeline message.
   *
   * @param op_index The index of the specified operator node in the query DAG
   *        for the pipelining block.
   * @param block The block id.
   * @param rel_id The ID of the relation that produced 'block'.
   * @param part_id The partition ID of 'block', if any. By default, a block
   *        blongs to the only partition (aka, no partition).
   **/
  void processDataPipelineMessage(const dag_node_index op_index,
                                  const block_id block,
                                  const relation_id rel_id,
                                  const partition_id part_id = 0);

  /**
   * @brief Fetch all work orders currently available in relational operator and
   *        store them internally.
   *
   * @param index The index of the relational operator to be processed in the
   *        query plan DAG.
   *
   * @return Whether any work order was generated by op.
   **/
  virtual bool fetchNormalWorkOrders(const dag_node_index index) = 0;

  /**
   * @brief Process the received work order feedback message and notify
   *        relational operator.
   *
   * @param op_index The index of the specified operator node in the query DAG
   *        for the feedback message.
   * @param message Feedback message from work order.
   **/
  void processFeedbackMessage(const dag_node_index op_index,
                              const WorkOrder::FeedbackMessage &message);

  /**
   * @brief Get the query status after processing an incoming message.
   *
   * @param op_index The index of the specified operator node in the query DAG
   *        for the incoming message.
   *
   * @return QueryStatusCode as determined after the message is processed.
   **/
  QueryStatusCode queryStatus(const dag_node_index op_index);

  /**
   * @brief Get the execution DAG visualizer.
   *
   * @return the execution DAG visualizer.
   **/
  ExecutionDAGVisualizer* dag_visualizer() {
    return dag_visualizer_.get();
  }

 protected:
  /**
   * @brief This function does the following things:
   *        1. Mark the given relational operator as "done".
   *        2. For all the dependents of this operator, check if the given
   *        operator is done producing output. If it's done, inform the
   *        dependents that they won't receive input anymore from the given
   *        operator.
   *        3. Check if all of their blocking dependencies are met. If so
   *        fetch normal work orders.
   *
   * @param index The index of the given relational operator in the DAG.
   **/
  void markOperatorFinished(const dag_node_index index);

  /**
   * @brief Check if all the blocking dependencies of the node at specified
   *        index have finished their execution.
   *
   * @note A blocking dependency is the one which is pipeline breaker. Output of
   *       a dependency can't be streamed to its dependent if the link between
   *       them is pipeline breaker.
   *
   * @param node_index The index of the specified node in the query DAG.
   *
   * @return True if all the blocking dependencies have finished their
   *         execution. False otherwise.
   **/
  inline bool checkAllBlockingDependenciesMet(
      const dag_node_index node_index) const {
    return blocking_dependencies_[node_index].empty();
  }

  /**
   * @brief Check if the rebuild operation is required for a given operator.
   *
   * @param index The index of the given operator in the DAG.
   *
   * @return True if the rebuild operation is required, false otherwise.
   **/
  inline bool checkRebuildRequired(const dag_node_index index) const {
    return query_exec_state_->isRebuildRequired(index);
  }

  /**
   * @brief Check if the rebuild operation for a given operator has been
   *        initiated.
   *
   * @param index The index of the given operator in the DAG.
   *
   * @return True if the rebuild operation has been initiated, false otherwise.
   **/
  inline bool checkRebuildInitiated(const dag_node_index index) const {
    return query_exec_state_->hasRebuildInitiated(index);
  }

  /**
   * @brief Get the query's current memory consumption in bytes.
   *
   * @note This method returns a best guess consumption, at the time of the call.
   **/
  virtual std::size_t getQueryMemoryConsumptionBytes() const {
    return 0;
  }

  std::unique_ptr<QueryHandle> query_handle_;

  const std::size_t query_id_;

  DAG<RelationalOperator, bool> *query_dag_;  // Owned by 'query_handle_'.
  const dag_node_index num_operators_in_dag_;

  // For all nodes, store their receiving dependents.
  std::vector<std::vector<dag_node_index>> output_consumers_;

  // For all nodes, store their pipeline breaking dependencies (if any).
  std::vector<std::unordered_set<dag_node_index>> blocking_dependencies_;

  std::vector<dag_node_index> non_dependent_operators_;

  std::unique_ptr<QueryExecutionState> query_exec_state_;

  std::unique_ptr<ExecutionDAGVisualizer> dag_visualizer_;

 private:
  /**
   * @brief Check if the given operator's normal execution is over.
   *
   * @note The conditions for a given operator's normal execution to get over:
   *       1. All of its  normal (i.e. non rebuild) WorkOrders have finished
   *       execution.
   *       2. The operator is done generating work orders.
   *       3. All of the dependencies of the given operator have been met.
   *
   * @param index The index of the given operator in the DAG.
   *
   * @return True if the normal execution of the given operator is over, false
   *         otherwise.
   **/
  virtual bool checkNormalExecutionOver(const dag_node_index index) const = 0;

  /**
   * @brief Initiate the rebuild process for partially filled blocks generated
   *        during the execution of the given operator.
   *
   * @param index The index of the given operator in the DAG.
   *
   * @return True if the rebuild is over immediately, i.e. the operator didn't
   *         generate any rebuild WorkOrders, false otherwise.
   **/
  virtual bool initiateRebuild(const dag_node_index index) = 0;

  /**
   * @brief Check if the rebuild operation for a given operator is over.
   *
   * @param index The index of the given operator in the DAG.
   *
   * @return True if the rebuild operation is over, false otherwise.
   **/
  virtual bool checkRebuildOver(const dag_node_index index) const = 0;

  // For all nodes, store their pipeline breaking dependents (if any).
  std::vector<std::vector<dag_node_index>> blocking_dependents_;
  std::vector<std::unordered_set<dag_node_index>> non_blocking_dependencies_;

  DISALLOW_COPY_AND_ASSIGN(QueryManagerBase);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_QUERY_MANAGER_BASE_HPP_
