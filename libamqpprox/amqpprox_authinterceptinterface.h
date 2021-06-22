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
#ifndef BLOOMBERG_AMQPPROX_AUTHINTERCEPTINTERFACE
#define BLOOMBERG_AMQPPROX_AUTHINTERCEPTINTERFACE

#include <functional>
#include <iostream>
#include <mutex>
#include <string>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

namespace Bloomberg {
namespace amqpprox {

/**
 * \brief Provide a virtual interface for authn/authz operations
 */
class AuthInterceptInterface {
  protected:
    boost::asio::io_service &d_ioService;
    mutable std::mutex       d_mutex;

  public:
    typedef std::function<void(const boost::system::error_code &ec,
                               const std::string &              responseText)>
        ReceiveResponseCb;

    // CREATORS
    explicit AuthInterceptInterface(boost::asio::io_service &ioService);

    virtual ~AuthInterceptInterface() = default;

    // MANIPULATORS
    virtual void sendRequest(const std::string        requestBody,
                             const ReceiveResponseCb &responseCb) = 0;

    // ACCESSORS
    virtual void print(std::ostream &os) const = 0;
    ///< Print information about route auth gate service
};

}
}

#endif
