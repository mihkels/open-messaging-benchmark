/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.rocketmq;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.jupiter.api.Test;

class RocketMQAclCompatibilityTest {
    @Test
    void signsRequestsWithTheCurrentRocketMqClient() {
        var request = RemotingCommand.createRequestCommand(0, null);
        var hook = new AclClientRPCHook(new SessionCredentials("access", "secret", "token"));

        hook.doBeforeRequest("localhost:9876", request);

        assertThat(request.getExtFields())
                .containsEntry("AccessKey", "access")
                .containsEntry("SecurityToken", "token");
        assertThat(request.getExtFields().get("Signature")).isNotBlank();
    }
}
