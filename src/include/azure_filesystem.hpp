#pragma once

#include "azure_parsed_url.hpp"
#include "azure_client_config.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <azure/core/datetime.hpp>
#include <ctime>
#include <cstdint>

namespace duckdb {

struct AzureReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
};

class AzureContextState : public ClientContextState {
public:
	const AzureReadOptions read_options;

public:
	virtual bool IsValid() const;
	void QueryEnd() override;

	template <class TARGET>
	TARGET &As() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &As() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	AzureContextState(const AzureReadOptions &read_options);

protected:
	bool is_valid;
};

class AzureStorageFileSystem;

class AzureFileHandle : public FileHandle {
public:
	virtual void PostConstruct();
	void Close() override {
	}

protected:
	AzureFileHandle(AzureStorageFileSystem &fs, string path, uint8_t flags, const AzureReadOptions &read_options);

public:
	const uint8_t flags;

	// File info
	idx_t length;
	time_t last_modified;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	const AzureReadOptions read_options;
};

class AzureStorageFileSystem : public FileSystem {
public:
	// FS methods
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
	                                        FileCompressionType compression = DEFAULT_COMPRESSION,
	                                        FileOpener *opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename) override {
		return false;
	}
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void FileSync(FileHandle &handle) override;

	void LoadFileInfo(AzureFileHandle &handle);

	string PathSeparator(const string &path) final {
		return "/";
	}

protected:
	virtual duckdb::unique_ptr<AzureFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                                         FileCompressionType compression, FileOpener *opener) = 0;
	virtual void ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) = 0;

	virtual const string &GetContextPrefix() const = 0;
	std::shared_ptr<AzureContextState> GetOrCreateStorageContext(FileOpener *opener, const string &path,
	                                                             const AzureParsedUrl &parsed_url);
	virtual std::shared_ptr<AzureContextState> CreateStorageContext(FileOpener *opener, const string &path,
	                                                                const AzureParsedUrl &parsed_url,
	                                                                const AzureClientConfig &config) = 0;

	virtual void LoadRemoteFileInfo(AzureFileHandle &handle) = 0;
	static AzureReadOptions ParseAzureReadOptions(FileOpener *opener);
	static time_t ToTimeT(const Azure::DateTime &dt);
};

} // namespace duckdb
