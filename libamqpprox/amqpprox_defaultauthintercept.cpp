/*
** Copyright 2020 Bloomberg Finance L.P.
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/

#include <amqpprox_defaultauthintercept.h>

#include <amqpprox_authinterceptinterface.h>

#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include <boost/asio.hpp>
#include <boost/beast.hpp>

namespace Bloomberg {
namespace amqpprox {

DefaultAuthIntercept::DefaultAuthIntercept(boost::asio::io_service &ioService)
: AuthInterceptInterface(ioService)
{
}

void DefaultAuthIntercept::sendRequest(const std::string,
                                       const ReceiveResponseCb &responseCb)
{
    auto cb = [responseCb] {
        responseCb(boost::beast::error_code(), "Default auth gate service");
    };
    boost::asio::post(d_ioService,
                      std::bind(responseCb,
                                boost::beast::error_code(),
                                "Default auth gate service"));
}

void DefaultAuthIntercept::print(std::ostream &os) const
{
    std::lock_guard<std::mutex> lg(d_mutex);
    os << "No auth service will be used to authn/authz client connections."
       << "\n";
}

}
}
