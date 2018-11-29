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

#ifndef QUICKSTEP_QUERY_EXECUTION_FOREMAN_DISTRIBUTED_HPP_
#define QUICKSTEP_QUERY_EXECUTION_FOREMAN_DISTRIBUTED_HPP_

#include <cstddef>
#include <cstdio>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/ForemanBase.hpp"
#include "query_execution/ShiftbossDirectory.hpp"
#include "storage/DataExchangerAsync.hpp"
#include "storage/StorageManager.hpp"
#include "utility/Macros.hpp"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class BlockLocator;
class CatalogDatabaseLite;
class QueryProcessor;

namespace serialization { class WorkOrderMessage; }

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief The Foreman receives queries from the main thread, messages from the
 *        policy enforcer and dispatches the work to Shiftbosses. It also
 *        receives work completion messages from Shiftbosses.
 **/
class ForemanDistributed final : public ForemanBase {
 public:
  /**
   * @brief Constructor.
   *
   * @param block_locator The block locator that manages block location info.
   * @param bus A pointer to the TMB.
   * @param catalog_database The catalog database where this query is executed.
   * @param query_processor The QueryProcessor to save catalog upon the query
   *        completion.
   * @param cpu_id The ID of the CPU to which the Foreman thread can be pinned.
   *
   * @note If cpu_id is not specified, Foreman thread can be possibly moved
   *       around on different CPUs by the OS.
  **/
  ForemanDistributed(
      const BlockLocator &block_locator,
      tmb::MessageBus *bus,
      CatalogDatabaseLite *catalog_database,
      QueryProcessor *query_processor,
      const int cpu_id = -1);

  ~ForemanDistributed() override {
    data_exchanger_.shutdown();
    storage_manager_.reset();
    data_exchanger_.join();
  }

  void printWorkOrderProfilingResults(const std::size_t query_id,
                                      std::FILE *out) const override;

 protected:
  void run() override;

 private:
  bool isAggregationRelatedWorkOrder(const serialization::WorkOrderMessage &proto,
                                     const std::size_t next_shiftboss_index_to_schedule,
                                     std::size_t *shiftboss_index_for_aggregation);

  bool isHashJoinRelatedWorkOrder(const serialization::WorkOrderMessage &proto,
                                  const std::size_t next_shiftboss_index_to_schedule,
                                  std::size_t *shiftboss_index_for_hash_join);

  bool isLipRelatedWorkOrder(const serialization::WorkOrderMessage &proto,
                             const std::size_t next_shiftboss_index_to_schedule,
                             std::size_t *shiftboss_index_for_lip);

  /**
   * @brief Dispatch schedulable WorkOrders, wrapped in WorkOrderMessages to the
   *        worker threads.
   *
   * @param messages The messages to be dispatched.
   **/
  void dispatchWorkOrderMessages(
      const std::vector<std::unique_ptr<serialization::WorkOrderMessage>> &messages);

  /**
   * @brief Send the given message to the specified worker.
   *
   * @param worker_index The logical index of the recipient worker in
   *        ShiftbossDirectory.
   * @param proto The WorkOrderMessage to be sent.
   **/
  void sendWorkOrderMessage(const std::size_t worker_index,
                            const serialization::WorkOrderMessage &proto);

  void processShiftbossRegistrationMessage(const tmb::client_id shiftboss_client_id,
                                           const std::size_t work_order_capacity);

  /**
   * @brief Check if we can collect new messages from the PolicyEnforcer.
   *
   * @param message_type The type of the last received message.
   **/
  bool canCollectNewMessages(const tmb::message_type_id message_type);

  // To get block locality info for scheduling.
  const BlockLocator &block_locator_;

  ShiftbossDirectory shiftboss_directory_;

  CatalogDatabaseLite *catalog_database_;

  // Used for '\analyze'.
  DataExchangerAsync data_exchanger_;
  std::unique_ptr<StorageManager> storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(ForemanDistributed);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_FOREMAN_DISTRIBUTED_HPP_
