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

cd package/target
TARBALL=$(ls openmessaging-benchmark-*-bin.tar.gz)
DIST=${TARBALL%-bin.tar.gz}
tar zxf "${TARBALL}"
mkdir -p "${DIST}/drivers/driver-jms/ext"
cp -p "${1}" "${DIST}/drivers/driver-jms/ext/"
rm "${TARBALL}"
tar zcf "${TARBALL}" "${DIST}"
