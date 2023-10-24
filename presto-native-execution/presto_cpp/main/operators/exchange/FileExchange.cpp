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
#include "presto_cpp/main/operators/exchange/FileExchange.h"
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/Configs.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

namespace {
inline std::string createExchangeFileName(
    const std::string& basePath,
    const std::string& exchangeId,
    int32_t partition,
    const std::string& writerId) {
  // Follow Spark's exchange file name format: exchange_exchangeId_0_reduceId
  return fmt::format(
      "{}/{}_{}_{}.bin",
      basePath,
      exchangeId,
      partition,
      writerId);
}

// This file is used to indicate that the exchange system is ready to be used for
// reading (acts as a sync point between readers if needed). Mostly used for
// test purposes.
const static std::string kReadyForReadFilename = "readyForRead";
}; // namespace

FileExchangeWriter::FileExchangeWriter(
    const std::string& basePath,
    const std::string& exchangeId,
    const std::string& writerId,
    uint32_t numPartitions,
    uint64_t maxBytesPerPartition,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool),
      numPartitions_(numPartitions),
      maxBytesPerPartition_(maxBytesPerPartition),
      basePath_(std::move(basePath)),
      exchangeId_(exchangeId),
      writerId_(writerId) {
  // Use resize/assign instead of resize(size, val).
  inProgressPartitions_.resize(numPartitions_);
  inProgressPartitions_.assign(numPartitions_, nullptr);
  inProgressSizes_.resize(numPartitions_);
  inProgressSizes_.assign(numPartitions_, 0);
  fileSystem_ = velox::filesystems::getFileSystem(basePath_, nullptr);
}

std::unique_ptr<velox::WriteFile>
FileExchangeWriter::getNextOutputFile(int32_t partition) {
  auto filename = nextAvailablePartitionFileName(basePath_, partition);
  return fileSystem_->openFileForWrite(filename);
}

std::string FileExchangeWriter::nextAvailablePartitionFileName(
    const std::string& basePath,
    int32_t partition) const {
  int fileIndex = 0;
  std::string filename;
  // TODO: consider to maintain the next to create file count in memory as we
  // always do cleanup when switch to a new root directory path.
  do {
    filename = createExchangeFileName(
        basePath, exchangeId_, partition, writerId_);
    if (!fileSystem_->exists(filename)) {
      break;
    }
    ++fileIndex;
  } while (true);

  return filename;
}

void FileExchangeWriter::storePartitionBlock(int32_t partition) {
  auto& buffer = inProgressPartitions_[partition];
  auto file = getNextOutputFile(partition);
  file->append(
      std::string_view(buffer->as<char>(), inProgressSizes_[partition]));
  file->close();
  inProgressPartitions_[partition].reset();
  inProgressSizes_[partition] = 0;
}

void FileExchangeWriter::collect(
    int32_t partition,
    std::string_view data) {
  using TRowSize = uint32_t;

  // synchronize appends from multiple threads of the writer task
  std::lock_guard wl(writeAppendLock);

  auto& buffer = inProgressPartitions_[partition];
  const TRowSize rowSize = data.size();
  const auto size = sizeof(TRowSize) + rowSize;

  // Check if there is enough space in the buffer.
  if ((buffer != nullptr) &&
      (inProgressSizes_[partition] + size >= buffer->capacity())) {
    storePartitionBlock(partition);
    // NOTE: the referenced 'buffer' will be reset in storePartitionBlock.
  }

  // Allocate buffer if needed.
  if (buffer == nullptr) {
    buffer = AlignedBuffer::allocate<char>(
        std::max((uint64_t)size, maxBytesPerPartition_), pool_);
    inProgressSizes_[partition] = 0;
    inProgressPartitions_[partition] = buffer;
  }

  // Copy data.
  auto offset = inProgressSizes_[partition];
  auto rawBuffer = buffer->asMutable<char>() + offset;

  *(TRowSize*)(rawBuffer) = folly::Endian::big(rowSize);
  ::memcpy(rawBuffer + sizeof(TRowSize), data.data(), rowSize);

  inProgressSizes_[partition] += size;
}

void FileExchangeWriter::noMoreData(bool success) {
  // Delete all exchange files on failure.
  if (!success) {
    cleanup();
  }
  for (auto i = 0; i < numPartitions_; ++i) {
    if (inProgressSizes_[i] > 0) {
      storePartitionBlock(i);
    }
  }
}

FileExchangeReader::FileExchangeReader(
    const std::string& basePath,
    const std::string& exchangeId,
    std::vector<std::string> partitionIds,
    std::vector<std::string> writerIds,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : basePath_(basePath),
      exchangeId_(exchangeId),
      partitionIds_(std::move(partitionIds)),
      writerIds_(std::move(writerIds)),
      pool_(pool) {
  fileSystem_ = velox::filesystems::getFileSystem(basePath_, nullptr);
}

bool FileExchangeReader::hasNext() {
  if (readPartitionFiles_.empty()) {
    readPartitionFiles_ = getReadPartitionFiles();
  }

  return readPartitionFileIndex_ < readPartitionFiles_.size();
}

BufferPtr FileExchangeReader::next() {
  auto filename = readPartitionFiles_[readPartitionFileIndex_];
  // We want to simulate exchange read failures.
  // Randomly, throw a file opening error by adding
  // arbitrary suffix to filename
  if (rand() % 20 == -1) {
    filename = filename + "badSuffixToIntroduceFailure";
  }

  auto file = fileSystem_->openFileForRead(filename);
  auto buffer = AlignedBuffer::allocate<char>(file->size(), pool_, 0);
  file->pread(0, file->size(), buffer->asMutable<void>());
  ++readPartitionFileIndex_;
  return buffer;
}

void FileExchangeReader::noMoreData(bool success) {
  // On failure, reset the index of the files to be read.
  if (!success) {
    readPartitionFileIndex_ = 0;
  }
}

std::vector<std::string> FileExchangeReader::getReadPartitionFiles()
    const {
  // Get rid of excess '/' characters in the path.
  auto trimmedBasePath = basePath_;
  while (trimmedBasePath.length() > 0 &&
         trimmedBasePath[trimmedBasePath.length() - 1] == '/') {
    trimmedBasePath.erase(trimmedBasePath.length() - 1, 1);
  }

  std::vector<std::string> partitionFiles;
  for (const auto& partitionId : partitionIds_) {
    for (const auto& writerId : writerIds_) {
      partitionFiles.push_back(
        fmt::format("{}/{}_{}_{}.bin", trimmedBasePath, exchangeId_, partitionId, writerId));
    }
  }

  return partitionFiles;
}

void FileExchangeWriter::cleanup() {
  auto files = fileSystem_->list(basePath_);
  for (auto& file : files) {
    fileSystem_->remove(file);
  }
}

using json = nlohmann::json;

// static
FileExchangeSinkInfo FileExchangeSinkInfo::deserialize(
    const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  FileExchangeSinkInfo exchangeInfo;
  jsonReadInfo.at("basePath").get_to(exchangeInfo.basePath);
  jsonReadInfo.at("exchangeId").get_to(exchangeInfo.exchangeId);
  jsonReadInfo.at("writerId").get_to(exchangeInfo.writerId);
  jsonReadInfo.at("numPartitions").get_to(exchangeInfo.numPartitions);
  return exchangeInfo;
}

FileExchangeSourceInfo FileExchangeSourceInfo::deserialize(
    const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  FileExchangeSourceInfo exchangeInfo;
  jsonReadInfo.at("basePath").get_to(exchangeInfo.basePath);
  jsonReadInfo.at("exchangeId").get_to(exchangeInfo.exchangeId);
  jsonReadInfo.at("partitionIds").get_to(exchangeInfo.partitionIds);
  jsonReadInfo.at("writerIds").get_to(exchangeInfo.writerIds);
  return exchangeInfo;
}

std::shared_ptr<ShuffleReader> FileExchangeFactory::createReader(
    const std::string& serializedStr,
    const int32_t /*partition*/,
    velox::memory::MemoryPool* pool) {
  const operators::FileExchangeSourceInfo readInfo =
      operators::FileExchangeSourceInfo::deserialize(serializedStr);
  return std::make_shared<operators::FileExchangeReader>(
      readInfo.basePath,
      readInfo.exchangeId,
      readInfo.partitionIds,
      readInfo.writerIds,
      pool);
}

std::shared_ptr<ShuffleWriter> FileExchangeFactory::createWriter(
    const std::string& serializedStr,
    velox::memory::MemoryPool* pool) {
  static const uint64_t maxBytesPerPartition =
      SystemConfig::instance()->localShuffleMaxPartitionBytes();
  const operators::FileExchangeSinkInfo writeInfo =
      operators::FileExchangeSinkInfo::deserialize(serializedStr);

  // we want to create only 1 writer per taskAttempt/writerId
  std::string shuffleWriterId = writeInfo.basePath + "_" + writeInfo.exchangeId + "_" + writeInfo.writerId;
  auto it = writersMap_.find(shuffleWriterId);
  if (it == writersMap_.end()) {
    std::shared_ptr<ShuffleWriter> writer = std::make_shared<operators::FileExchangeWriter>(
            writeInfo.basePath,
            writeInfo.exchangeId,
            writeInfo.writerId,
            writeInfo.numPartitions,
            maxBytesPerPartition,
            pool);
    writersMap_.emplace(shuffleWriterId, writer);
  }
  auto itFinal = writersMap_.find(shuffleWriterId);
  return itFinal->second;
}

} // namespace facebook::presto::operators
