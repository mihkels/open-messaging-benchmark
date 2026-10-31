#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Adds DataStax Starlight for JMS to the JMS plugin's ext/ directory in the distribution.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$SCRIPT_DIR/package-common.sh"

prepare_distribution
PULSAR_JMS_VERSION=6.0.11
PULSAR_JMS_SHA256=d4c744c8e5bdedb8f373ece959ebf2256128fc31248d5c2db909bce913ed7f3a
PULSAR_JMS_JAR="$PACKAGE_WORK_DIR/pulsar-jms-all-$PULSAR_JMS_VERSION.jar"
curl -fsSL "https://repo1.maven.org/maven2/com/datastax/oss/pulsar-jms-all/$PULSAR_JMS_VERSION/pulsar-jms-all-$PULSAR_JMS_VERSION.jar" \
    -o "$PULSAR_JMS_JAR"
verify_sha256 "$PULSAR_JMS_JAR" "$PULSAR_JMS_SHA256"
for old_jar in "$EXT_DIR"/pulsar-jms-all-*.jar; do
    [ -e "$old_jar" ] || continue
    rm "$old_jar"
done
cp "$PULSAR_JMS_JAR" "$EXT_DIR/"
replace_distribution
