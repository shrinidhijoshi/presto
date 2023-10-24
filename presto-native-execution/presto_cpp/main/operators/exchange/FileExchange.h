/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"

namespace facebook::presto::operators {

// FileExchangeSinkInfo is used for containing exchange write information.
// This struct is a 1:1 strict API mapping to
// presto-spark-base/src/main/java/com/facebook/presto/spark/execution/PrestoSparkFileExchangeSinkInfo.java
// Please refrain changes to this API class. If any changes have to be made to
// this struct, one should make sure to make corresponding changes in the above
// Java classes and its corresponding serde functionalities.
struct FileExchangeSinkInfo {
  std::string basePath;
  std::string exchangeId;
  std::string writerId;
  uint32_t numPartitions;

  /// Deserializes exchange information that is used by LocalPersistentShuffle.
  /// Structures are assumed to be encoded in JSON format.
  static FileExchangeSinkInfo deserialize(const std::string& info);
};

// FileExchangeSourceInfo is used for containing exchange read metadata
// This struct is a 1:1 strict API mapping to
// presto-spark-base/src/main/java/com/facebook/presto/spark/execution/PrestoSparkFileExchangeSourceInfo.java.
// Please refrain changes to this API class. If any changes have to be made to
// this struct, one should make sure to make corresponding changes in the above
// Java classes and its corresponding serde functionalities.
struct FileExchangeSourceInfo {
  std::string basePath;
  std::string exchangeId;
  std::vector<std::string> partitionIds;
  std::vector<std::string> writerIds;

  /// Deserializes exchange information that is used by LocalPersistentShuffle.
  /// Structures are assumed to be encoded in JSON format.
  static FileExchangeSourceInfo deserialize(const std::string& info);
};

/// This class is a persistent exchange server that implements
/// ShuffleInterface for read and write and also uses generalized Velox
/// file system to maintain its state and data.
///
/// Except for in-progress blocks of current output vectors in the writer,
/// each produced vector is stored as a binary file of unsafe rows. Each block
/// filename reflects the partition and sequence number of the block (vector)
/// for that partition. For example <ROOT_PATH>/10_12.bin is the 12th (block)
/// vector in partition #10.
///
/// The class also uses Velox filesystem to figure out the number of written
/// exchange files for each partition. This enables the multi-threaded or
/// multi-process use scenarios as long as each producer or consumer is assigned
/// to a distinct group of partition IDs. Each of them can create an instance of
/// this class (pointing to the same root path) to read and write exchange data.
class FileExchangeWriter : public ShuffleWriter {
 public:
  FileExchangeWriter(
      const std::string& basePath,
      const std::string& exchangeId,
      const std::string& writerId,
      uint32_t numPartitions,
      uint64_t maxBytesPerPartition,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);

  void collect(int32_t partition, std::string_view data) override;

  void noMoreData(bool success) override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    // Fake counter for testing only.
    return {{"local.write", 2345}};
  }

 private:
  // Finds and creates the next file for writing the next block of the
  // given 'partition'.
  std::unique_ptr<velox::WriteFile> getNextOutputFile(int32_t partition);

  // Writes the in-progress block to the given partition.
  void storePartitionBlock(int32_t partition);

  // Deletes all the files in the root directory.
  void cleanup();

  // find next available partition file name to store exchange data
  std::string nextAvailablePartitionFileName(
      const std::string& basePath,
      int32_t partition) const;

  // Used to make sure files created by this thread have unique names.
  velox::memory::MemoryPool* FOLLY_NONNULL pool_;
  const uint32_t numPartitions_;
  const uint64_t maxBytesPerPartition_;
  // The top directory of the exchange files and its file system.
  const std::string basePath_;
  const std::string exchangeId_;
  const std::string writerId_;

  /// The latest written block buffers and sizes.
  std::mutex writeAppendLock;
  std::vector<velox::BufferPtr> inProgressPartitions_;
  std::vector<size_t> inProgressSizes_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
};

class FileExchangeReader : public ShuffleReader {
 public:
  FileExchangeReader(
      const std::string& basePath,
      const std::string& exchangeId,
      std::vector<std::string> partitionIds_,
      std::vector<std::string> writerIds,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);

  bool hasNext() override;

  velox::BufferPtr next() override;

  void noMoreData(bool success) override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    // Fake counter for testing only.
    return {{"local.read", 123}};
  }

 private:
  // Returns all created exchange files for 'partition_'.
  std::vector<std::string> getReadPartitionFiles() const;

  const std::string basePath_;
  const std::string exchangeId_;
  const std::vector<std::string> partitionIds_;
  const std::vector<std::string> writerIds_;
  velox::memory::MemoryPool* FOLLY_NONNULL pool_;

  // Latest read block (file) index in 'readPartitionFiles_' for 'partition_'.
  size_t readPartitionFileIndex_{0};

  // List of generated files for 'partition_'.
  std::vector<std::string> readPartitionFiles_;

  // The top directory of the exchange files and its file system.
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
};

class FileExchangeFactory : public ShuffleInterfaceFactory {
 public:
  static constexpr folly::StringPiece kShuffleName{"file"};
  std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedStr,
      const int32_t partition,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedStr,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

 private:
  std::unordered_map<std::string, std::shared_ptr<ShuffleWriter>> writersMap_;
};

} // namespace facebook::presto::operators
