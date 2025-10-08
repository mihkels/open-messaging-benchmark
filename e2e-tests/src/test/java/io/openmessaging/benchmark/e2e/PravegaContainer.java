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
package io.openmessaging.benchmark.e2e;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class PravegaContainer extends GenericContainer<PravegaContainer> {

    private static final int CONTROLLER_PORT = 9090;
    private static final int SEGMENT_STORE_PORT = 12345;
    private static final String DEFAULT_IMAGE = "pravega/pravega:0.13.0";
    private static final String HOST_IP = "127.0.0.1";

    public PravegaContainer() {
        this(DEFAULT_IMAGE);
    }

    public PravegaContainer(String imageName) {
        super(DockerImageName.parse(imageName));

        withCommand("standalone");
        withEnv("JAVA_OPTS", "-Xmx1g");
        withEnv("HOST_IP", HOST_IP);

        // Use the protected method in our subclass
        addFixedExposedPort(CONTROLLER_PORT, CONTROLLER_PORT);
        addFixedExposedPort(SEGMENT_STORE_PORT, SEGMENT_STORE_PORT);
    }

    public String getControllerURI() {
        return "tcp://" + HOST_IP + ":" + CONTROLLER_PORT;
    }

    public String getSegmentStoreEndpoint() {
        return HOST_IP + ":" + SEGMENT_STORE_PORT;
    }
}
