#pragma once

#include "azure_parsed_url.hpp"
#include "azure_client_config.hpp"
#include "duckdb/common/file_opener.hpp"
#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/storage/files/datalake/datalake_service_client.hpp>
#include <string>

namespace duckdb {

Azure::Storage::Blobs::BlobServiceClient ConnectToBlobStorageAccount(FileOpener *opener, const std::string &path,
                                                                     const AzureParsedUrl &azure_parsed_url,
                                                                     const AzureClientConfig &config);

Azure::Storage::Files::DataLake::DataLakeServiceClient
ConnectToDfsStorageAccount(FileOpener *opener, const std::string &path, const AzureParsedUrl &azure_parsed_url,
                           const AzureClientConfig &config);

} // namespace duckdb
