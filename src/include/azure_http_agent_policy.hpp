#pragma once

#include <azure/core/http/policies/policy.hpp>
#include <memory>
#include <string>

namespace duckdb {
class UserAgentPolicy final : public Azure::Core::Http::Policies::HttpPolicy {
public:
	UserAgentPolicy(std::string user_agent);

	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     Azure::Core::Http::Policies::NextHttpPolicy next_policy,
	                                                     Azure::Core::Context const &context) const override;

	std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> Clone() const override;

private:
	const std::string user_agent;
};
} // namespace duckdb
