#pragma once

#include "azure_parsed_url.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <azure/core/datetime.hpp>
#include <azure/core/etag.hpp>
#include <ctime>
#include <cstdint>
#include <string>

namespace duckdb {

struct AzureReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
};

struct AzureWriteOptions {
	idx_t block_size;
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

	void AssertOpenForWriting() const {
		if (!flags.OpenForWriting()) {
			throw IOException("%s hasn't been open with any writing flags", path);
		}
	}

protected:
	AzureFileHandle(AzureStorageFileSystem &fs, string path, FileOpenFlags flags, const AzureReadOptions &read_options,
	                const AzureWriteOptions &write_options);

public:
	const FileOpenFlags flags;

	// File info
	idx_t length;
	time_t last_modified;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;

	// Write buffer
	duckdb::unique_ptr<data_t[]> write_buffer;

	// Read info
	idx_t buffer_available; // TODO rename & check usage
	idx_t buffer_idx; // TODO rename & check usage
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	const AzureReadOptions read_options;
	const AzureWriteOptions write_options;
};

class AzureStorageFileSystem : public FileSystem {
public:
	// FS methods
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override = 0;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return false;
	}
	FileType GetFileType(FileHandle &handle) override {
		return FileType::FILE_TYPE_REGULAR;
	}
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void FileSync(FileHandle &handle) override;

	void LoadFileInfo(AzureFileHandle &handle);
	virtual void CreateOrOverwrite(AzureFileHandle &handle) = 0;
	virtual void CreateIfNotExists(AzureFileHandle &handle) = 0;

protected:
	virtual duckdb::unique_ptr<AzureFileHandle> CreateHandle(const string &path, FileOpenFlags flags,
	                                                         optional_ptr<FileOpener> opener) = 0;
	virtual void ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) = 0;

	virtual const string &GetContextPrefix() const = 0;
	std::shared_ptr<AzureContextState> GetOrCreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                             const AzureParsedUrl &parsed_url);
	virtual std::shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                                const AzureParsedUrl &parsed_url) = 0;

	virtual void LoadRemoteFileInfo(AzureFileHandle &handle) = 0;

	static AzureReadOptions ParseAzureReadOptions(optional_ptr<FileOpener> opener);
	static time_t ToTimeT(const Azure::DateTime &dt);
};

} // namespace duckdb
