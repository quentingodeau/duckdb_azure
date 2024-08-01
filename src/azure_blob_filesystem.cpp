#include "azure_blob_filesystem.hpp"

#include "azure_filesystem.hpp"
#include "azure_parsed_url.hpp"
#include "azure_storage_account_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include <azure/storage/blobs.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

namespace duckdb {

const string AzureBlobStorageFileSystem::SCHEME = "azure";
const string AzureBlobStorageFileSystem::SHORT_SCHEME = "az";

const string AzureBlobStorageFileSystem::PATH_PREFIX = "azure://";
const string AzureBlobStorageFileSystem::SHORT_PATH_PREFIX = "az://";

// taken from s3fs.cpp TODO: deduplicate!
static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end) {

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		if (!LikeFun::Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}

//////// AzureBlobContextState ////////
AzureBlobContextState::AzureBlobContextState(Azure::Storage::Blobs::BlobServiceClient client,
                                             const AzureReadOptions &azure_read_options,
                                             const AzureWriteOptions &azure_write_options)
    : AzureContextState(azure_read_options, azure_write_options), service_client(std::move(client)) {
}

Azure::Storage::Blobs::BlobContainerClient
AzureBlobContextState::GetBlobContainerClient(const std::string &blobContainerName) const {
	return service_client.GetBlobContainerClient(blobContainerName);
}

//////// AzureBlobStorageFileHandle ////////
AzureBlobStorageFileHandle::AzureBlobStorageFileHandle(AzureBlobStorageFileSystem &fs, string path, FileOpenFlags flags,
                                                       const AzureReadOptions &read_options,
                                                       const AzureWriteOptions &write_options,
                                                       Azure::Storage::Blobs::BlobClient blob_client)
    : AzureFileHandle(fs, std::move(path), flags, read_options, write_options), blob_client(std::move(blob_client)) {
}

//////// AzureBlobStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureBlobStorageFileSystem::CreateHandle(const string &path, FileOpenFlags flags,
                                                                     optional_ptr<FileOpener> opener) {
	auto parsed_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, parsed_url);
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(parsed_url.container);
	auto blob_client = container.GetBlockBlobClient(parsed_url.path);

	auto handle = make_uniq<AzureBlobStorageFileHandle>(*this, path, flags, storage_context->read_options, storage_context->write_options, 
	                                                    std::move(blob_client));
	handle->PostConstruct();
	return std::move(handle);
}

bool AzureBlobStorageFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind(PATH_PREFIX, 0) * fpath.rfind(SHORT_PATH_PREFIX, 0) == 0;
}

vector<string> AzureBlobStorageFileSystem::Glob(const string &path, FileOpener *opener) {
	if (!opener) {
		throw InternalException("Cannot do Azure storage Glob without FileOpener");
	}

	auto azure_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, azure_url);

	// Azure matches on prefix, not glob pattern, so we take a substring until the first wildcard
	auto first_wildcard_pos = azure_url.path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		return {path};
	}

	string shared_path = azure_url.path.substr(0, first_wildcard_pos);
	auto container_client = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(azure_url.container);

	const auto pattern_splits = StringUtil::Split(azure_url.path, "/");
	vector<string> result;

	Azure::Storage::Blobs::ListBlobsOptions options;
	options.Prefix = shared_path;

	const auto path_result_prefix =
	    (azure_url.is_fully_qualified ? (azure_url.prefix + azure_url.storage_account_name + '.' + azure_url.endpoint +
	                                     '/' + azure_url.container)
	                                  : (azure_url.prefix + azure_url.container));
	while (true) {
		// Perform query
		Azure::Storage::Blobs::ListBlobsPagedResponse res;
		try {
			res = container_client.ListBlobs(options);
		} catch (Azure::Storage::StorageException &e) {
			throw IOException("AzureStorageFileSystem Read to %s failed with %s Reason Phrase: %s", path, e.ErrorCode,
			                  e.ReasonPhrase);
		}

		// Assuming that in the majority of the case it's wildcard
		result.reserve(result.size() + res.Blobs.size());

		// Ensure that the retrieved element match the expected pattern
		for (const auto &key : res.Blobs) {
			vector<string> key_splits = StringUtil::Split(key.Name, "/");
			bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

			if (is_match) {
				auto result_full_url = path_result_prefix + '/' + key.Name;
				result.push_back(result_full_url);
			}
		}

		// Manage Azure pagination
		if (res.NextPageToken) {
			options.ContinuationToken = res.NextPageToken;
		} else {
			break;
		}
	}

	return result;
}

void AzureBlobStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &hfh = handle.Cast<AzureBlobStorageFileHandle>();

	auto res = hfh.blob_client.GetProperties();
	hfh.length = res.Value.BlobSize;
	hfh.last_modified = ToTimeT(res.Value.LastModified);
}

bool AzureBlobStorageFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	// Do not use open file here because it might Actually
	auto parsed_url = ParseUrl(filename);
	auto container_client = GetOrCreateStorageContext(opener, filename, parsed_url)
	                            ->As<AzureBlobContextState>()
	                            .GetBlobContainerClient(parsed_url.container);
	try {
		auto blob_client = container_client.GetBlobClient(parsed_url.path);
		blob_client.GetProperties();
		return true;
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

void AzureBlobStorageFileSystem::ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out,
                                           idx_t buffer_out_len) {
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();

	try {
		// Specify the range
		Azure::Core::Http::HttpRange range;
		range.Offset = (int64_t)file_offset;
		range.Length = buffer_out_len;
		Azure::Storage::Blobs::DownloadBlobToOptions options;
		options.Range = range;
		options.TransferOptions.Concurrency = afh.read_options.transfer_concurrency;
		options.TransferOptions.InitialChunkSize = afh.read_options.transfer_chunk_size;
		options.TransferOptions.ChunkSize = afh.read_options.transfer_chunk_size;
		auto res = afh.blob_client.DownloadTo((uint8_t *)buffer_out, buffer_out_len, options);

	} catch (const Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to '%s' failed with %s Reason Phrase: %s", afh.path,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

void AzureBlobStorageFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = handle.Cast<AzureBlobStorageFileHandle>();
	hfh.blob_client.AsBlockBlobClient();
	//.StageBlock(const std::string &blockId, Azure::Core::IO::BodyStream &content); // TODO cache the client in the
	// filehandler
	// TODO implement!!
}

void AzureBlobStorageFileSystem::FileSync(FileHandle &handle) {
	// TODO implement!!
}

void AzureBlobStorageFileSystem::CreateOrOverwrite(AzureFileHandle &handle) {
	// TODO implement!!
	auto &hfh = handle.Cast<AzureBlobStorageFileHandle>();
	hfh.AssertOpenForWriting();

	hfh.blob_client.DeleteIfExists();
	handle.length = 0;
	handle.last_modified = 0;
}

void AzureBlobStorageFileSystem::CreateIfNotExists(AzureFileHandle &handle) {
	handle.AssertOpenForWriting();
	// TODO implement!!
	// TODO get file info if exists
}

std::shared_ptr<AzureContextState> AzureBlobStorageFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                                    const string &path,
                                                                                    const AzureParsedUrl &parsed_url) {
	auto azure_read_options = ParseAzureReadOptions(opener);
	auto azure_write_options = AzureWriteOptions(); // TODO parse them!

	return std::make_shared<AzureBlobContextState>(ConnectToBlobStorageAccount(opener, path, parsed_url),
	                                               azure_read_options, azure_write_options);
}

} // namespace duckdb
