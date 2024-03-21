#include "azure_http_agent_policy.hpp"

namespace duckdb {
UserAgentPolicy::UserAgentPolicy(std::string user_agent) : user_agent(user_agent) {
}

std::unique_ptr<Azure::Core::Http::RawResponse>
UserAgentPolicy::Send(Azure::Core::Http::Request &request, Azure::Core::Http::Policies::NextHttpPolicy next_policy,
                      Azure::Core::Context const &context) const {
	request.SetHeader("User-Agent", user_agent);
	return next_policy.Send(request, context);
}
std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> UserAgentPolicy::Clone() const {
	return std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> {new UserAgentPolicy(user_agent)};
}
} // namespace duckdb
