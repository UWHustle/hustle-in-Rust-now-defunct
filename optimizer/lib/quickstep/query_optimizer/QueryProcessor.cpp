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

#include "query_optimizer/QueryProcessor.hpp"

#include <fstream>
#include <memory>
#include <string>

#include "catalog/Catalog.hpp"
#include "catalog/Catalog.pb.h"
#include "query_optimizer/Optimizer.hpp"
#include "query_optimizer/OptimizerContext.hpp"

using std::ifstream;
using std::ofstream;

namespace quickstep {

class QueryHandle;

void QueryProcessor::generateQueryHandle(const ParseStatement &statement,
                                         QueryHandle *query_handle) {
  optimizer::OptimizerContext optimizer_context;

  optimizer_.generateQueryHandle(statement, getDefaultDatabase(), &optimizer_context, query_handle);

  if (optimizer_context.is_catalog_changed() && !catalog_altered_) {
    catalog_altered_ = true;
  }

  ++query_id_;
}

void QueryProcessor::saveCatalog() {
  if (catalog_altered_) {
    ofstream catalog_file(catalog_filename_.c_str());

    if (!catalog_->getProto().SerializeToOstream(&catalog_file)) {
      throw UnableToWriteCatalog(catalog_filename_);
    }

    catalog_file.close();

    catalog_altered_ = false;
  }
}

void QueryProcessor::loadCatalog() {
  ifstream catalog_file(catalog_filename_.c_str());
  if (!catalog_file.good()) {
    throw UnableToReadCatalog(catalog_filename_);
  }

  serialization::Catalog catalog_proto;
  if (!catalog_proto.ParseFromIstream(&catalog_file)) {
    throw CatalogNotProto(catalog_filename_);
  }
  catalog_file.close();
  catalog_ = std::make_unique<Catalog>(catalog_proto);

  catalog_altered_ = false;
}

void QueryProcessor::findReferencedBaseRelationsInQuery(
    const ParseStatement &statement, QueryHandle *query_handle) {
  optimizer_.findReferencedBaseRelations(statement, getDefaultDatabase(), query_handle);
}

}  // namespace quickstep
