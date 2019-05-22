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

#include "query_execution/ProbabilityStore.hpp"

#include <cstddef>
#include <vector>

#include "gtest/gtest.h"

namespace quickstep {

TEST(ProbabilityStoreTest, CountTest) {
  ProbabilityStore store;
  EXPECT_EQ(0u, store.getNumObjects());
  const std::size_t kKey = 0;
  store.addOrUpdateObjectNewDenominator(kKey, 1, 1);
  EXPECT_EQ(1u, store.getNumObjects());
  store.removeObject(kKey);
  EXPECT_EQ(0u, store.getNumObjects());

  std::vector<std::size_t> objects {3, 5, 7, 9};
  std::vector<float> numerators {1, 2, 2, 5};
  const std::size_t kNewDenominator = std::accumulate(numerators.begin(), numerators.end(), 0);
  store.addOrUpdateObjectsNewDenominator(objects, numerators, kNewDenominator);

  EXPECT_EQ(objects.size(), store.getNumObjects());
}

TEST(ProbabilityStoreTest, IndividualProbabilityTest) {
  ProbabilityStore store;
  std::vector<std::size_t> objects {3, 5, 7, 9};
  std::vector<float> numerators {1, 2, 2, 5};
  const std::size_t kNewDenominator = std::accumulate(numerators.begin(), numerators.end(), 0);
  store.addOrUpdateObjectsNewDenominator(objects, numerators, kNewDenominator);

  for (std::size_t object_num = 0; object_num < objects.size(); ++object_num) {
    EXPECT_EQ(numerators[object_num] / static_cast<float>(kNewDenominator),
              store.getIndividualProbability(objects[object_num]));
  }
}

TEST(ProbabilityStoreTest, PickRandomKeyTest) {
  ProbabilityStore store;
  std::vector<std::size_t> objects {3, 5, 7, 9};
  std::vector<float> numerators {1, 2, 2, 5};
  const std::size_t kNewDenominator = std::accumulate(numerators.begin(), numerators.end(), 0);
  store.addOrUpdateObjectsNewDenominator(objects, numerators, kNewDenominator);

  const std::size_t kNumTrials = 10;
  while (!objects.empty()) {
    for (std::size_t trial_num = 0; trial_num < kNumTrials; ++trial_num) {
      const std::size_t picked_key = store.pickRandomKey();
      const auto it = std::find(objects.begin(), objects.end(), picked_key);
      EXPECT_TRUE(it != objects.end());
    }
    const std::size_t key_to_be_removed = objects.back();
    store.removeObject(key_to_be_removed);
    objects.pop_back();
    EXPECT_EQ(objects.size(), store.getNumObjects());
  }
}

TEST(ProbabilityStoreTest, RemoveObjectTest) {
  ProbabilityStore store;
  std::vector<std::size_t> objects {3, 5, 7, 9};
  std::vector<float> numerators {1, 2, 2, 5};
  const std::size_t kNewDenominator = std::accumulate(numerators.begin(), numerators.end(), 0);
  store.addOrUpdateObjectsNewDenominator(objects, numerators, kNewDenominator);

  for (std::size_t object_num = 0; object_num < objects.size(); ++object_num) {
    EXPECT_EQ(numerators[object_num] / static_cast<float>(kNewDenominator),
              store.getIndividualProbability(objects[object_num]));
  }

  // Remove last object "9", with numerator 5.
  store.removeObject(objects.back());
  objects.pop_back();
  numerators.pop_back();
  const float expected_new_denominator =
      std::accumulate(numerators.begin(), numerators.end(), 0);

  EXPECT_EQ(expected_new_denominator, store.getDenominator());
  for (std::size_t object_num = 0; object_num < objects.size(); ++object_num) {
    EXPECT_EQ(numerators[object_num] / static_cast<float>(expected_new_denominator),
              store.getIndividualProbability(objects[object_num]));
  }
}

}  // namespace quickstep
