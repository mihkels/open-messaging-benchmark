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

# The one argument is the path to the Confluent JMS client fat jar on your local system.
# The jar is added to the JMS plugin's ext/ directory in the distribution.
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -f "$1" ] || [ ! -r "$1" ]; then
    echo "Usage: bash driver-jms/package-kafka.sh /path/to/kafka-jms-client.jar" >&2
    exit 2
fi
case "$1" in
    /*) KAFKA_JMS_JAR=$1 ;;
    *) KAFKA_JMS_JAR="$PWD/$1" ;;
esac
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$SCRIPT_DIR/package-common.sh"

prepare_distribution
cp -p "$KAFKA_JMS_JAR" "$EXT_DIR/"
replace_distribution
