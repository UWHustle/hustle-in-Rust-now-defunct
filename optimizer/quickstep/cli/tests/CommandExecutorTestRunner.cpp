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

#include "cli/tests/CommandExecutorTestRunner.hpp"

#include <cstdio>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "cli/CommandExecutor.hpp"
#include "cli/DropRelation.hpp"
#include "cli/PrintToScreen.hpp"
#include "parser/ParseStatement.hpp"
#include "query_execution/AdmitRequestMessage.hpp"
#include "query_execution/ForemanSingleNode.hpp"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "query_optimizer/QueryProcessor.hpp"
#include "utility/MemStream.hpp"
#include "utility/SqlError.hpp"

#include "glog/logging.h"

#include "tmb/tagged_message.h"

namespace quickstep {

class CatalogRelation;

namespace O = ::quickstep::optimizer;

void CommandExecutorTestRunner::runTestCase(
    const std::string &input, const std::set<std::string> &options,
    std::string *output) {
  // TODO(qzeng): Test multi-threaded query execution when we have a Sort operator.

  VLOG(4) << "Test SQL(s): " << input;

  MemStream output_stream;
  sql_parser_.feedNextBuffer(new std::string(input));

  while (true) {
    ParseResult result = sql_parser_.getNextStatement();
    if (result.condition != ParseResult::kSuccess) {
      if (result.condition == ParseResult::kError) {
        *output = result.error_message;
      }
      break;
    } else {
      const ParseStatement &parse_statement = *result.parsed_statement;
      std::printf("%s\n", parse_statement.toString().c_str());
      try {
        if (parse_statement.getStatementType() == ParseStatement::kCommand) {
          cli::executeCommand(
              *result.parsed_statement,
              *query_processor_->getDefaultDatabase(),
              main_thread_client_id_,
              foreman_->getBusClientID(),
              &bus_,
              &storage_manager_,
              query_processor_.get(),
              output_stream.file());
        } else {
          const CatalogRelation *query_result_relation = nullptr;
          {
            auto query_handle = std::make_unique<QueryHandle>(0 /* query_id */, main_thread_client_id_);
            query_processor_->generateQueryHandle(parse_statement, query_handle.get());
            query_result_relation = query_handle->getQueryResultRelation();

            QueryExecutionUtil::ConstructAndSendAdmitRequestMessage(
                main_thread_client_id_, foreman_->getBusClientID(), query_handle.release(), &bus_);
          }

          // Receive workload completion message from Foreman.
          const AnnotatedMessage annotated_msg =
              bus_.Receive(main_thread_client_id_, 0, true);
          const TaggedMessage &tagged_message = annotated_msg.tagged_message;
          DCHECK_EQ(kWorkloadCompletionMessage, tagged_message.message_type());
          if (query_result_relation) {
            PrintToScreen::PrintRelation(*query_result_relation,
                                         &storage_manager_,
                                         output_stream.file());
            DropRelation::Drop(*query_result_relation,
                               query_processor_->getDefaultDatabase(),
                               &storage_manager_);
          }
        }
      } catch (const SqlError &error) {
        *output = error.formatMessage(input);
        break;
      }
    }
  }

  if (output->empty()) {
    *output = output_stream.str();
  }
}

}  // namespace quickstep
