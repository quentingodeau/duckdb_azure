#include "azure_dfs_filesystem.hpp"
#include "azure_filesystem.hpp"
#include "azure_parsed_url.hpp"
#include "azure_storage_account_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include <algorithm>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <azure/storage/files/datalake/datalake_file_system_client.hpp>
#include <azure/storage/files/datalake/datalake_directory_client.hpp>
#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>
#include <azure/storage/files/datalake/datalake_responses.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
const string AzureDfsStorageFileSystem::SCHEME = "abfss";
const string AzureDfsStorageFileSystem::PATH_PREFIX = "abfss://";

inline static bool IsDfsScheme(const string &fpath) {
	return fpath.rfind("abfss://", 0) == 0;
}

static void Walk(const Azure::Storage::Files::DataLake::DataLakeFileSystemClient &fs, const std::string &path,
                 const string &path_pattern, std::size_t end_match, std::vector<std::string> *out_result) {
	auto directory_client = fs.GetDirectoryClient(path);

	bool recursive = false;
	const auto double_star = path_pattern.rfind("**", end_match);
	if (double_star != std::string::npos) {
		if (path_pattern.length() > end_match) {
			throw NotImplementedException("abfss do not manage recursive lookup patterns, %s is therefor illegal, only "
			                              "pattern ending by ** are allowed.",
			                              path_pattern);
		}
		// pattern end with a **, perform recursive listing from this point
		recursive = true;
	}

	Azure::Storage::Files::DataLake::ListPathsOptions options;
	while (true) {
		auto res = directory_client.ListPaths(recursive, options);

		for (const auto &elt : res.Paths) {
			if (elt.IsDirectory) {
				if (!recursive) { // Only perform recursive call if we are not already processing recursive result
					if (LikeFun::Glob(elt.Name.data(), elt.Name.length(), path_pattern.data(), end_match)) {
						if (end_match >= path_pattern.length()) {
							// Skip, no way there will be matches anymore
							continue;
						}
						Walk(fs, elt.Name, path_pattern,
						     std::min(path_pattern.length(), path_pattern.find('/', end_match + 1)), out_result);
					}
				}
			} else {
				// File
				if (LikeFun::Glob(elt.Name.data(), elt.Name.length(), path_pattern.data(), path_pattern.length())) {
					out_result->push_back(elt.Name);
				}
			}
		}

		if (res.NextPageToken) {
			options.ContinuationToken = res.NextPageToken;
		} else {
			break;
		}
	}
}

//////// AzureDfsContextState ////////
AzureDfsContextState::AzureDfsContextState(Azure::Storage::Files::DataLake::DataLakeServiceClient client,
                                           const AzureReadOptions &azure_read_options,
                                           const AzureWriteOptions &azure_write_options)
    : AzureContextState(azure_read_options, azure_write_options), service_client(std::move(client)) {
}

Azure::Storage::Files::DataLake::DataLakeFileSystemClient
AzureDfsContextState::GetDfsFileSystemClient(const std::string &file_system_name) const {
	return service_client.GetFileSystemClient(file_system_name);
}

//////// AzureDfsContextState ////////
AzureDfsStorageFileHandle::AzureDfsStorageFileHandle(AzureDfsStorageFileSystem &fs, string path, FileOpenFlags flags,
                                                     const AzureReadOptions &read_options,
                                                     const AzureWriteOptions &write_options,
                                                     Azure::Storage::Files::DataLake::DataLakeFileClient client)
    : AzureFileHandle(fs, std::move(path), flags, read_options, write_options), file_client(std::move(client)) {
}

//////// AzureDfsStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureDfsStorageFileSystem::CreateHandle(const string &path, FileOpenFlags flags,
                                                                    optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, parsed_url);

	auto handle = make_uniq<AzureDfsStorageFileHandle>(*this, path, flags, storage_context->read_options,
	                                                   storage_context->write_options,
	                                                   CreateFileClient(opener, path, parsed_url));
	handle->PostConstruct();
	return std::move(handle);
}

Azure::Storage::Files::DataLake::DataLakeFileClient
AzureDfsStorageFileSystem::CreateFileClient(optional_ptr<FileOpener> opener, const string &path,
                                            const AzureParsedUrl &parsed_url) {
	auto storage_context = GetOrCreateStorageContext(opener, path, parsed_url);
	auto file_system_client = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(parsed_url.container);
	return file_system_client.GetFileClient(parsed_url.path);
}

bool AzureDfsStorageFileSystem::CanHandleFile(const string &fpath) {
	return IsDfsScheme(fpath);
}

// Read operation
vector<string> AzureDfsStorageFileSystem::Glob(const string &path, FileOpener *opener) {
	if (!opener) {
		throw InternalException("Cannot do Azure storage Glob without FileOpener");
	}

	auto azure_url = ParseUrl(path);

	// If path does not contains any wildcard, we assume that an absolute path therefor nothing to do
	auto first_wildcard_pos = azure_url.path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		return {path};
	}

	// The path contains wildcard try to list file with the minimum calls
	auto storage_context = GetOrCreateStorageContext(opener, path, azure_url);
	auto dfs_filesystem_client =
	    storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(azure_url.container);

	auto index_root_dir = azure_url.path.rfind('/', first_wildcard_pos);
	if (index_root_dir == string::npos) {
		index_root_dir = 0;
	}
	auto shared_path = azure_url.path.substr(0, index_root_dir);

	std::vector<std::string> result;
	Walk(dfs_filesystem_client, shared_path,
	     // pattern to match
	     azure_url.path, std::min(azure_url.path.length(), azure_url.path.find('/', index_root_dir + 1)),
	     // output result
	     &result);

	if (!result.empty()) {
		const auto path_result_prefix =
		    (azure_url.is_fully_qualified ? (azure_url.prefix + azure_url.storage_account_name + '.' +
		                                     azure_url.endpoint + '/' + azure_url.container)
		                                  : (azure_url.prefix + azure_url.container)) +
		    '/';
		for (auto &elt : result) {
			elt = path_result_prefix + elt;
		}
	}

	return result;
}

void AzureDfsStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &hfh = handle.Cast<AzureDfsStorageFileHandle>();

	auto res = hfh.file_client.GetProperties();
	hfh.length = res.Value.FileSize;
	hfh.last_modified = ToTimeT(res.Value.LastModified);
}

void AzureDfsStorageFileSystem::ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out,
                                          idx_t buffer_out_len) {
	using Azure::Core::Http::HttpRange;
	using Azure::Storage::StorageException;
	using Azure::Storage::Files::DataLake::DownloadFileToOptions;

	auto &afh = handle.Cast<AzureDfsStorageFileHandle>();
	try {
		// Specify the range
		HttpRange range;
		range.Offset = static_cast<int64_t>(file_offset);
		range.Length = buffer_out_len;
		DownloadFileToOptions options;
		options.Range = range;
		options.TransferOptions.Concurrency = afh.read_options.transfer_concurrency;
		options.TransferOptions.InitialChunkSize = afh.read_options.transfer_chunk_size;
		options.TransferOptions.ChunkSize = afh.read_options.transfer_chunk_size;
		auto res = afh.file_client.DownloadTo((uint8_t *)buffer_out, buffer_out_len, options);

	} catch (const StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to '%s' failed with %s Reason Phrase: %s", afh.path,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

bool AzureDfsStorageFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(filename);
	auto file_client = CreateFileClient(opener, filename, parsed_url);

	try {
		auto properties = file_client.GetProperties();
		return !properties.Value.IsDirectory;
	} catch (const Azure::Storage::StorageException &e) {
		if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
			return false;
		} else {
			throw IOException(
			    "%s Failed to check if file exits '%s' failed with code'%s', Reason Phrase: '%s', Message: '%s'",
			    GetName(), filename, e.ErrorCode, e.ReasonPhrase, e.Message);
		}
	}
}

bool AzureDfsStorageFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(directory);
	auto file_client = CreateFileClient(opener, directory, parsed_url);

	try {
		auto properties = file_client.GetProperties();
		return properties.Value.IsDirectory;
	} catch (const Azure::Storage::StorageException &e) {
		if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
			return false;
		} else {
			throw IOException(
			    "%s Failed to check if directory exits '%s' failed with code'%s', Reason Phrase: '%s', Message: '%s'",
			    GetName(), directory, e.ErrorCode, e.ReasonPhrase, e.Message);
		}
	}
}

// Write operation
void AzureDfsStorageFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	using Azure::Core::IO::MemoryBodyStream;
	using Azure::Storage::Files::DataLake::AppendFileOptions;

	auto &hfh = handle.Cast<AzureDfsStorageFileHandle>();
	hfh.AssertOpenForWriting();

	if (location != hfh.length) {
		throw NotImplementedException("Non-sequential write not supported!");
	}

	MemoryBodyStream mbs(reinterpret_cast<const uint8_t *>(buffer), nr_bytes);
	AppendFileOptions options;
	options.Flush = false;
	hfh.file_client.Append(mbs, location, options);
	hfh.length += nr_bytes;
}

void AzureDfsStorageFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(directory);
	auto storage_context = GetOrCreateStorageContext(opener, directory, parsed_url);
	auto file_system_client = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(parsed_url.container);
	auto directory_client = file_system_client.GetDirectoryClient(directory);
	directory_client.CreateIfNotExists();
}

void AzureDfsStorageFileSystem::FileSync(FileHandle &handle) {
	auto &hfh = handle.Cast<AzureDfsStorageFileHandle>();
	hfh.AssertOpenForWriting();
	auto response = hfh.file_client.Flush(hfh.length);
	hfh.last_modified = ToTimeT(response.Value.LastModified);
}

void AzureDfsStorageFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(filename);
	auto file_client = CreateFileClient(opener, filename, parsed_url);
	file_client.DeleteIfExists();
}

void AzureDfsStorageFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(directory);
	auto storage_context = GetOrCreateStorageContext(opener, directory, parsed_url);
	auto file_system_client = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(parsed_url.container);
	auto directory_client = file_system_client.GetDirectoryClient(directory);
	directory_client.DeleteRecursiveIfExists();
}

void AzureDfsStorageFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	auto source_url = ParseUrl(source);
	auto target_url = ParseUrl(target);

	if (source_url.container != target_url.container &&
	    source_url.storage_account_name != target_url.storage_account_name) {
		throw NotImplementedException("Cannot move files ('%s' => '%s') into a different container/storage account.",
		                              source, target);
	}

	auto storage_context = GetOrCreateStorageContext(opener, source, source_url);
	auto file_system_client = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(source_url.container);
	file_system_client.RenameFile(source_url.path, target_url.path);
}

void AzureDfsStorageFileSystem::CreateOrOverwrite(AzureFileHandle &handle) {
	using Azure::Storage::Files::DataLake::UploadFileFromOptions;
	auto &hfh = handle.Cast<AzureDfsStorageFileHandle>();
	hfh.AssertOpenForWriting();

	hfh.file_client.DeleteIfExists();
	auto response = hfh.file_client.Create();

	handle.length = 0;
	handle.last_modified = ToTimeT(response.Value.LastModified);
}

void AzureDfsStorageFileSystem::CreateIfNotExists(AzureFileHandle &handle) {
	using Azure::Storage::Files::DataLake::CreateFileOptions;
	auto &hfh = handle.Cast<AzureDfsStorageFileHandle>();
	hfh.AssertOpenForWriting();

	CreateFileOptions options;
	// options.
	auto response = hfh.file_client.CreateIfNotExists();

	handle.length = response.Value.FileSize.ValueOr(0);
	handle.last_modified = ToTimeT(response.Value.LastModified);
}

// Other operation
std::shared_ptr<AzureContextState> AzureDfsStorageFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                                   const string &path,
                                                                                   const AzureParsedUrl &parsed_url) {
	auto azure_read_options = ParseAzureReadOptions(opener);
	auto azure_write_options = AzureWriteOptions(); // TODO parse them!

	return std::make_shared<AzureDfsContextState>(ConnectToDfsStorageAccount(opener, path, parsed_url),
	                                              azure_read_options, azure_write_options);
}

} // namespace duckdb
