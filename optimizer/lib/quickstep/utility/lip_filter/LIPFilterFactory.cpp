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

#include "utility/lip_filter/LIPFilterFactory.hpp"

#include <cstddef>
#include <cstdint>

#include "utility/lip_filter/LIPFilter.pb.h"
#include "utility/lip_filter/BitVectorExactFilter.hpp"
#include "utility/lip_filter/SingleIdentityHashFilter.hpp"

#include "glog/logging.h"

namespace quickstep {

LIPFilter* LIPFilterFactory::ReconstructFromProto(const serialization::LIPFilter &proto) {
  switch (proto.lip_filter_type()) {
    case serialization::LIPFilterType::BIT_VECTOR_EXACT_FILTER: {
      const std::size_t attr_size =
          proto.GetExtension(serialization::BitVectorExactFilter::attribute_size);
      const std::int64_t min_value =
          proto.GetExtension(serialization::BitVectorExactFilter::min_value);
      const std::int64_t max_value =
          proto.GetExtension(serialization::BitVectorExactFilter::max_value);
      const bool is_anti_filter =
          proto.GetExtension(serialization::BitVectorExactFilter::is_anti_filter);

      switch (attr_size) {
        case 1:
          if (is_anti_filter) {
            return new BitVectorExactFilter<std::int8_t, true>(min_value, max_value);
          } else {
            return new BitVectorExactFilter<std::int8_t, false>(min_value, max_value);
          }
        case 2:
          if (is_anti_filter) {
            return new BitVectorExactFilter<std::int16_t, true>(min_value, max_value);
          } else {
            return new BitVectorExactFilter<std::int16_t, false>(min_value, max_value);
          }
        case 4:
          if (is_anti_filter) {
            return new BitVectorExactFilter<std::int32_t, true>(min_value, max_value);
          } else {
            return new BitVectorExactFilter<std::int32_t, false>(min_value, max_value);
          }
        case 8:
          if (is_anti_filter) {
            return new BitVectorExactFilter<std::int64_t, true>(min_value, max_value);
          } else {
            return new BitVectorExactFilter<std::int64_t, false>(min_value, max_value);
          }
        default:
          LOG(FATAL) << "Invalid attribute size for BitVectorExactFilter: "
                     << attr_size;
      }
    }
    case serialization::LIPFilterType::SINGLE_IDENTITY_HASH_FILTER: {
      const std::size_t attr_size =
          proto.GetExtension(serialization::SingleIdentityHashFilter::attribute_size);
      const std::size_t filter_cardinality =
          proto.GetExtension(serialization::SingleIdentityHashFilter::filter_cardinality);

      if (attr_size >= 8) {
        return new SingleIdentityHashFilter<std::uint64_t>(filter_cardinality);
      } else if (attr_size >= 4) {
        return new SingleIdentityHashFilter<std::uint32_t>(filter_cardinality);
      } else if (attr_size >= 2) {
        return new SingleIdentityHashFilter<std::uint16_t>(filter_cardinality);
      } else {
        return new SingleIdentityHashFilter<std::uint8_t>(filter_cardinality);
      }
    }
    // TODO(jianqiao): handle the BLOOM_FILTER and EXACT_FILTER implementations.
    default:
      LOG(FATAL) << "Unsupported LIP filter type: "
                 << serialization::LIPFilterType_Name(proto.lip_filter_type());
  }
  return nullptr;
}

bool LIPFilterFactory::ProtoIsValid(const serialization::LIPFilter &proto) {
  switch (proto.lip_filter_type()) {
    case serialization::LIPFilterType::BIT_VECTOR_EXACT_FILTER: {
      const std::size_t attr_size =
          proto.GetExtension(serialization::BitVectorExactFilter::attribute_size);
      const std::int64_t min_value =
          proto.GetExtension(serialization::BitVectorExactFilter::min_value);
      const std::int64_t max_value =
          proto.GetExtension(serialization::BitVectorExactFilter::max_value);
      return (attr_size != 0 && max_value >= min_value);
    }
    case serialization::LIPFilterType::SINGLE_IDENTITY_HASH_FILTER: {
      const std::size_t attr_size =
          proto.GetExtension(serialization::SingleIdentityHashFilter::attribute_size);
      const std::size_t filter_cardinality =
          proto.GetExtension(serialization::SingleIdentityHashFilter::filter_cardinality);
      return (attr_size != 0 && filter_cardinality != 0);
    }
    default:
      LOG(FATAL) << "Unsupported LIP filter type: "
                 << serialization::LIPFilterType_Name(proto.lip_filter_type());
  }
  return false;
}

}  // namespace quickstep
