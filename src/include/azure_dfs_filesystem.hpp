#pragma once

#include "azure_filesystem.hpp"
#include "duckdb/common/file_opener.hpp"
#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_file_system_client.hpp>
#include <azure/storage/files/datalake/datalake_service_client.hpp>
#include <string>
#include <vector>

namespace duckdb {

class AzureDfsContextState : public AzureContextState {
public:
	AzureDfsContextState(Azure::Storage::Files::DataLake::DataLakeServiceClient client,
	                     const AzureReadOptions &azure_read_options);
	Azure::Storage::Files::DataLake::DataLakeFileSystemClient
	GetDfsFileSystemClient(const std::string &file_system_name) const;

private:
	Azure::Storage::Files::DataLake::DataLakeServiceClient service_client;
};

class AzureDfsStorageFileSystem;

class AzureDfsStorageFileHandle : public AzureFileHandle {
public:
	AzureDfsStorageFileHandle(AzureDfsStorageFileSystem &fs, string path, FileOpenFlags flags,
	                          const AzureReadOptions &read_options,
	                          Azure::Storage::Files::DataLake::DataLakeFileClient client);
	~AzureDfsStorageFileHandle() override = default;

public:
	Azure::Storage::Files::DataLake::DataLakeFileClient file_client;
};

class AzureDfsStorageFileSystem : public AzureStorageFileSystem {
public:
	vector<string> Glob(const string &path, FileOpener* opener = nullptr) override;

	bool CanHandleFile(const string &fpath) override;
	string GetName() const override {
		return "AzureDfsStorageFileSystem";
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void FileSync(FileHandle &handle) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

	void CreateOrOverwrite(AzureFileHandle &handle) override;
	void CreateIfNotExists(AzureFileHandle &handle) override;

public:
	static const string SCHEME;
	static const string PATH_PREFIX;

protected:
	// From AzureFilesystem
	const string &GetContextPrefix() const override {
		return PATH_PREFIX;
	}
	std::shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                        const AzureParsedUrl &parsed_url) override;
	duckdb::unique_ptr<AzureFileHandle> CreateHandle(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	Azure::Storage::Files::DataLake::DataLakeFileClient CreateFileClient(optional_ptr<FileOpener> opener, const string &path,
	                                                                     const AzureParsedUrl &parsed_url);

	// From AzureFilesystem
	void LoadRemoteFileInfo(AzureFileHandle &handle) override;
	void ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) override;
};

} // namespace duckdb
