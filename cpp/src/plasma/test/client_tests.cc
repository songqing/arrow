// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <random>
#include <thread>

#include "arrow/test-util.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"

#include "gtest/gtest.h"

namespace plasma {

std::string test_executable;  // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer& object_buffer,
                             const std::vector<uint8_t>& metadata,
                             const std::vector<uint8_t>& data) {
  arrow::test::AssertBufferEqual(*object_buffer.metadata, metadata);
  arrow::test::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStore : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.
  void SetUp() {
    uint64_t seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 rng(static_cast<uint32_t>(seed));
    std::string store_index = std::to_string(rng());
    store_socket_name_ = "/tmp/store" + store_index;
    // store_socket_name_ = "/tmp/store1234";

    std::string plasma_directory =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_command = plasma_directory + "/plasma_store -m 1000000000 -s " +
                                 store_socket_name_ + " 1> /dev/null 2> /dev/null &";
    system(plasma_command.c_str());
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
    ARROW_CHECK_OK(client2_.Connect(store_socket_name_, ""));
  }
  virtual void TearDown() {
    ARROW_CHECK_OK(client_.Disconnect());
    ARROW_CHECK_OK(client2_.Disconnect());
    // Kill all plasma_store processes
    // TODO should only kill the processes we launched
#ifdef COVERAGE_BUILD
    // Ask plasma_store to exit gracefully and give it time to write out
    // coverage files
    system("killall -TERM plasma_store");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    system("killall -KILL plasma_store");
  }

  void CreateObject(PlasmaClient& client, const ObjectID& object_id,
                    const std::vector<uint8_t>& metadata,
                    const std::vector<uint8_t>& data) {
    std::shared_ptr<Buffer> data_buffer;
    ARROW_CHECK_OK(client.Create(object_id, data.size(), &metadata[0], metadata.size(),
                                 &data_buffer));
    for (size_t i = 0; i < data.size(); i++) {
      data_buffer->mutable_data()[i] = data[i];
    }
    ARROW_CHECK_OK(client.Seal(object_id));
    ARROW_CHECK_OK(client.Release(object_id));
  }

  const std::string& GetStoreSocketName() const { return store_socket_name_; }

 protected:
  PlasmaClient client_;
  PlasmaClient client2_;
  std::string store_socket_name_;
};

TEST_F(TestPlasmaStore, NewSubscriberTest) {
  PlasmaClient local_client, local_client2;

  ARROW_CHECK_OK(local_client.Connect(store_socket_name_, ""));
  ARROW_CHECK_OK(local_client2.Connect(store_socket_name_, ""));

  ObjectID object_id = ObjectID::from_random();

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      local_client.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(local_client.Seal(object_id));

  // Test that new subscriber client2 can receive notifications about existing objects.
  int fd = -1;
  ARROW_CHECK_OK(local_client2.Subscribe(&fd));
  ASSERT_GT(fd, 0);

  ObjectID object_id2 = ObjectID::from_random();
  int64_t data_size2 = 0;
  int64_t metadata_size2 = 0;
  ARROW_CHECK_OK(
      local_client2.GetNotification(fd, &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(object_id, object_id2);
  ASSERT_EQ(data_size, data_size2);
  ASSERT_EQ(metadata_size, metadata_size2);

  // Delete the object.
  ARROW_CHECK_OK(local_client.Release(object_id));
  ARROW_CHECK_OK(local_client.Delete(object_id));

  ARROW_CHECK_OK(
      local_client2.GetNotification(fd, &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(object_id, object_id2);
  ASSERT_EQ(-1, data_size2);
  ASSERT_EQ(-1, metadata_size2);

  ARROW_CHECK_OK(local_client2.Disconnect());
  ARROW_CHECK_OK(local_client.Disconnect());
}

TEST_F(TestPlasmaStore, SealErrorsTest) {
  ObjectID object_id = ObjectID::from_random();

  Status result = client_.Seal(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectNonexistent());

  // Create object.
  std::vector<uint8_t> data(100, 0);
  CreateObject(client_, object_id, {42}, data);

  // Trying to seal it again.
  result = client_.Seal(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectAlreadySealed());
}

TEST_F(TestPlasmaStore, DeleteTest) {
  ObjectID object_id = ObjectID::from_random();

  // Test for deleting non-existance object.
  Status result = client_.Delete(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectNonexistent());

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id));

  // Object is in use, can't be delete.
  result = client_.Delete(object_id);
  ASSERT_TRUE(result.IsUnknownError());

  // Avoid race condition of Plasma Manager waiting for notification.
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.Delete(object_id));
}

TEST_F(TestPlasmaStore, ContainsTest) {
  ObjectID object_id = ObjectID::from_random();

  // Test for object non-existence.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create object.
  std::vector<uint8_t> data(100, 0);
  CreateObject(client_, object_id, {42}, data);
  // Avoid race condition of Plasma Manager waiting for notification.
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
}

TEST_F(TestPlasmaStore, GetTest) {
  std::vector<ObjectBuffer> object_buffers;

  ObjectID object_id = ObjectID::from_random();

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_FALSE(object_buffers[0].metadata);
  ASSERT_FALSE(object_buffers[0].data);
  EXPECT_FALSE(client_.IsInUse(object_id));

  // Test for the object being in local Plasma store.
  // First create object.
  std::vector<uint8_t> data = {3, 5, 6, 7, 9};
  CreateObject(client_, object_id, {42}, data);
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));

  object_buffers.clear();
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 0);
  AssertObjectBufferEqual(object_buffers[0], {42}, {3, 5, 6, 7, 9});

  // Metadata keeps object in use
  {
    auto metadata = object_buffers[0].metadata;
    object_buffers.clear();
    ::arrow::test::AssertBufferEqual(*metadata, {42});
    ARROW_CHECK_OK(client_.FlushReleaseHistory());
    EXPECT_TRUE(client_.IsInUse(object_id));
  }
  // Object is automatically released
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));
}

TEST_F(TestPlasmaStore, LegacyGetTest) {
  // Test for old non-releasing Get() variant
  ObjectID object_id = ObjectID::from_random();
  {
    ObjectBuffer object_buffer;

    // Test for object non-existence.
    ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
    ASSERT_FALSE(object_buffer.metadata);
    ASSERT_FALSE(object_buffer.data);
    EXPECT_FALSE(client_.IsInUse(object_id));

    // First create object.
    std::vector<uint8_t> data = {3, 5, 6, 7, 9};
    CreateObject(client_, object_id, {42}, data);
    ARROW_CHECK_OK(client_.FlushReleaseHistory());
    EXPECT_FALSE(client_.IsInUse(object_id));

    ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
    AssertObjectBufferEqual(object_buffer, {42}, {3, 5, 6, 7, 9});
  }
  // Object needs releasing manually
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_TRUE(client_.IsInUse(object_id));
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));
}

TEST_F(TestPlasmaStore, MultipleGetTest) {
  ObjectID object_id1 = ObjectID::from_random();
  ObjectID object_id2 = ObjectID::from_random();
  std::vector<ObjectID> object_ids = {object_id1, object_id2};
  std::vector<ObjectBuffer> object_buffers;

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id1, data_size, metadata, metadata_size, &data));
  data->mutable_data()[0] = 1;
  ARROW_CHECK_OK(client_.Seal(object_id1));

  ARROW_CHECK_OK(client_.Create(object_id2, data_size, metadata, metadata_size, &data));
  data->mutable_data()[0] = 2;
  ARROW_CHECK_OK(client_.Seal(object_id2));

  ARROW_CHECK_OK(client_.Get(object_ids, -1, &object_buffers));
  ASSERT_EQ(object_buffers[0].data->data()[0], 1);
  ASSERT_EQ(object_buffers[1].data->data()[0], 2);
}

TEST_F(TestPlasmaStore, AbortTest) {
  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

  // Test object abort.
  // First create object.
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  uint8_t* data_ptr;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  data_ptr = data->mutable_data();
  // Write some data.
  for (int64_t i = 0; i < data_size / 2; i++) {
    data_ptr[i] = static_cast<uint8_t>(i % 4);
  }
  // Attempt to abort. Test that this fails before the first release.
  Status status = client_.Abort(object_id);
  ASSERT_TRUE(status.IsInvalid());
  // Release, then abort.
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_TRUE(client_.IsInUse(object_id));

  ARROW_CHECK_OK(client_.Abort(object_id));
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));

  // Test for object non-existence after the abort.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

  // Create the object successfully this time.
  CreateObject(client_, object_id, {42, 43}, {1, 2, 3, 4, 5});

  // Test that we can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  AssertObjectBufferEqual(object_buffers[0], {42, 43}, {1, 2, 3, 4, 5});
  ARROW_CHECK_OK(client_.Release(object_id));
}

TEST_F(TestPlasmaStore, MultipleClientTest) {
  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_TRUE(object_buffers[0].data);
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = ObjectID::from_random();
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  ARROW_CHECK_OK(client2_.Get({object_id}, -1, &object_buffers));
  ASSERT_TRUE(object_buffers[0].data);
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
}

TEST_F(TestPlasmaStore, QueuePushAndGetTest) {

  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 10 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  uint8_t item1[] = { 1, 2, 3, 4, 5 };
  int64_t item1_size = sizeof(item1);
  ARROW_CHECK_OK(client2_.PushQueueItem(object_id, item1, item1_size));

  uint8_t item2[] = { 6, 7, 8, 9 };
  int64_t item2_size = sizeof(item2);
  ARROW_CHECK_OK(client2_.PushQueueItem(object_id, item2, item2_size));

  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;
  uint64_t seq_id = -1;

  ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id));
  ASSERT_TRUE(seq_id == 1);
  ASSERT_TRUE(buff_size == item1_size);
  for (auto i = 0; i < buff_size; i++) {
    ASSERT_TRUE(buff[i] == item1[i]);
  }
   
  ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id));
  ASSERT_TRUE(seq_id == 2);
  ASSERT_TRUE(buff_size == item2_size);
  for (auto i = 0; i < buff_size; i++) {
    ASSERT_TRUE(buff[i] == item2[i]);
  }
}

TEST_F(TestPlasmaStore, QueueCreateAndGetTest) {

  ObjectID object_id = ObjectID::from_random();
  ObjectBuffer object_buffer;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 10 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  uint64_t seq_id = -1;

  uint8_t item1[] = { 1, 2, 3, 4, 5 };
  int64_t item1_size = sizeof(item1);

  ARROW_CHECK_OK(client2_.CreateQueueItem(object_id, item1_size, &data, seq_id));
  memcpy(data->mutable_data(), item1, item1_size);
  client2_.SealQueueItem(object_id, seq_id, data);
  

  uint8_t item2[] = { 6, 7, 8, 9 };
  int64_t item2_size = sizeof(item2);
  ARROW_CHECK_OK(client2_.CreateQueueItem(object_id, item2_size, &data, seq_id));
  memcpy(data->mutable_data(), item2, item2_size);
  client2_.SealQueueItem(object_id, seq_id, data);

  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;

  ARROW_CHECK_OK(client_.GetQueueItem(object_id, &object_buffer, seq_id));
  ASSERT_TRUE(seq_id == 1);
  ASSERT_TRUE(object_buffer.data->size() == item1_size);
  for (auto i = 0; i < item1_size; i++) {
    ASSERT_TRUE(object_buffer.data->data()[i] == item1[i]);
  }
  
  std::shared_ptr<Buffer> buffer;
  ARROW_CHECK_OK(client_.GetQueueItem(object_id, &buffer, seq_id));
  ASSERT_TRUE(seq_id == 2);
  ASSERT_TRUE(buffer->size() == item2_size);
  for (auto i = 0; i < item2_size; i++) {
    ASSERT_TRUE(buffer->data()[i] == item2[i]);
  }
}

TEST_F(TestPlasmaStore, QueueBatchPushAndGetTest) {

  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 1024 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  std::vector<uint64_t> items;
  items.resize(3000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  for (int i = 0; i < items.size(); i++) {
    uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
    ARROW_CHECK_OK(client2_.PushQueueItem(object_id, data, data_size));
  }

  for (int i = 0; i < items.size(); i++) {
    uint8_t* buff = nullptr;
    uint32_t buff_size = 0;
    uint64_t seq_id = -1;

    ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id));
    ASSERT_TRUE(seq_id == i + 1);
    ASSERT_TRUE(buff_size == sizeof(uint64_t));
    uint64_t value = *(uint64_t*)(buff);
    ASSERT_TRUE(value == items[i]);
  }
}

TEST_F(TestPlasmaStore, QueueBatchCreateAndGetTest) {

  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 1024 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  std::vector<uint64_t> items;
  items.resize(3000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  for (int i = 0; i < items.size(); i++) {
    uint64_t seq_id = -1;
    uint8_t* item = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t item_size = static_cast<uint32_t>(sizeof(uint64_t));
    
    ARROW_CHECK_OK(client2_.CreateQueueItem(object_id, item_size, &data, seq_id));
    memcpy(data->mutable_data(), item, item_size);
    client2_.SealQueueItem(object_id, seq_id, data);
  }

  for (int i = 0; i < items.size(); i++) {
    uint8_t* buff = nullptr;
    uint32_t buff_size = 0;
    uint64_t seq_id = -1;

    ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id));
    ASSERT_TRUE(seq_id == i + 1);
    ASSERT_TRUE(buff_size == sizeof(uint64_t));
    uint64_t value = *(uint64_t*)(buff);
    ASSERT_TRUE(value == items[i]);
  }
}

TEST_F(TestPlasmaStore, ManyObjectTest) {
  // Create many objects on the first client. Seal one third, abort one third,
  // and leave the last third unsealed.
  std::vector<ObjectID> object_ids;
  for (int i = 0; i < 100; i++) {
    ObjectID object_id = ObjectID::from_random();
    object_ids.push_back(object_id);

    // Test for object non-existence on the first client.
    bool has_object;
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_FALSE(has_object);

    // Test for the object being in local Plasma store.
    // First create and seal object on the first client.
    int64_t data_size = 100;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));

    if (i % 3 == 0) {
      // Seal one third of the objects.
      ARROW_CHECK_OK(client_.Seal(object_id));
      // Test that the first client can get the object.
      ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
      ASSERT_TRUE(has_object);
    } else if (i % 3 == 1) {
      // Abort one third of the objects.
      ARROW_CHECK_OK(client_.Release(object_id));
      ARROW_CHECK_OK(client_.Abort(object_id));
    }
  }
  // Disconnect the first client. All unsealed objects should be aborted.
  ARROW_CHECK_OK(client_.Disconnect());

  // Check that the second client can query the object store for the first
  // client's objects.
  int i = 0;
  for (auto const& object_id : object_ids) {
    bool has_object;
    ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
    if (i % 3 == 0) {
      // The first third should be sealed.
      ASSERT_TRUE(has_object);
    } else {
      // The rest were aborted, so the object is not in the store.
      ASSERT_FALSE(has_object);
    }
    i++;
  }
}

#ifndef ARROW_NO_DEPRECATED_API
TEST_F(TestPlasmaStore, DeprecatedApiTest) {
  int64_t default_delay = PLASMA_DEFAULT_RELEASE_DELAY;
  ARROW_CHECK(default_delay == plasma::kPlasmaDefaultReleaseDelay);
}
#endif  // ARROW_NO_DEPRECATED_API

#ifdef PLASMA_GPU
using arrow::gpu::CudaBuffer;
using arrow::gpu::CudaBufferReader;
using arrow::gpu::CudaBufferWriter;

namespace {

void AssertCudaRead(const std::shared_ptr<Buffer>& buffer,
                    const std::vector<uint8_t>& expected_data) {
  std::shared_ptr<CudaBuffer> gpu_buffer;
  const size_t data_size = expected_data.size();

  ARROW_CHECK_OK(CudaBuffer::FromBuffer(buffer, &gpu_buffer));
  ASSERT_EQ(gpu_buffer->size(), data_size);

  CudaBufferReader reader(gpu_buffer);
  uint8_t read_data[data_size];
  int64_t read_data_size;
  ARROW_CHECK_OK(reader.Read(data_size, &read_data_size, read_data));
  ASSERT_EQ(read_data_size, data_size);

  for (size_t i = 0; i < data_size; i++) {
    ASSERT_EQ(read_data[i], expected_data[i]);
  }
}

}  // namespace

TEST_F(TestPlasmaStore, GetGPUTest) {
  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_FALSE(object_buffers[0].data);

  // Test for the object being in local Plasma store.
  // First create object.
  uint8_t data[] = {4, 5, 3, 1};
  int64_t data_size = sizeof(data);
  uint8_t metadata[] = {42};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data_buffer;
  std::shared_ptr<CudaBuffer> gpu_buffer;
  ARROW_CHECK_OK(
      client_.Create(object_id, data_size, metadata, metadata_size, &data_buffer, 1));
  ARROW_CHECK_OK(CudaBuffer::FromBuffer(data_buffer, &gpu_buffer));
  CudaBufferWriter writer(gpu_buffer);
  ARROW_CHECK_OK(writer.Write(data, data_size));
  ARROW_CHECK_OK(client_.Seal(object_id));

  object_buffers.clear();
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 1);
  // Check data
  AssertCudaRead(object_buffers[0].data, {4, 5, 3, 1});
  // Check metadata
  AssertCudaRead(object_buffers[0].metadata, {42});
}

TEST_F(TestPlasmaStore, MultipleClientGPUTest) {
  ObjectID object_id = ObjectID::from_random();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = ObjectID::from_random();
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  object_buffers.clear();
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client2_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 1);
  AssertCudaRead(object_buffers[0].metadata, {5});
}

#endif  // PLASMA_GPU

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
