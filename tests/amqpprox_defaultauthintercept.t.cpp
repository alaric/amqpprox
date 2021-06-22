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

#include <gmock/gmock.h>

#include <iostream>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

using Bloomberg::amqpprox::DefaultAuthIntercept;

TEST(DefaultAuthIntercept, Breathing)
{
    boost::asio::io_service ioService;
    DefaultAuthIntercept    defaultAuth(ioService);
    ioService.run();
}

TEST(DefaultAuthIntercept, SendRequest)
{
    boost::asio::io_service ioService;
    DefaultAuthIntercept    defaultAuth(ioService);
    const std::string       requestBody = "{\"vhost\":\"test-vhost\"}";
    auto responseCb = [](const boost::system::error_code &returnCode,
                         const std::string &              responseText) {
        ASSERT_EQ(returnCode, boost::system::error_code());
        ASSERT_EQ(responseText, "Default auth gate service");
    };
    defaultAuth.sendRequest(requestBody, responseCb);
    ioService.run();
}

TEST(DefaultAuthIntercept, Print)
{
    boost::asio::io_service ioService;
    DefaultAuthIntercept    defaultAuth(ioService);
    ioService.run();
    std::ostringstream oss;
    defaultAuth.print(oss);
    EXPECT_EQ(
        oss.str(),
        "No auth service will be used to authn/authz client connections.\n");
}
