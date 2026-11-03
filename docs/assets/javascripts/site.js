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

// Material renders the site name in the header as plain text; link it to the home page like the logo.
(function () {
  "use strict";

  var logo = document.querySelector(".md-header__button.md-logo");
  var name = document.querySelector(".md-header__topic:first-child .md-ellipsis");
  if (!logo || !name) {
    return;
  }
  var link = document.createElement("a");
  link.href = logo.getAttribute("href");
  link.className = "omb-site-name";
  link.textContent = name.textContent.trim();
  name.textContent = "";
  name.appendChild(link);
})();
