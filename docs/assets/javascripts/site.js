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

// Custom Google Analytics events. Material keeps its gtag function private and only creates
// window.dataLayer once the visitor has accepted analytics cookies, so without consent this
// sends nothing.
(function () {
  "use strict";

  function track(name, params) {
    if (!window.dataLayer) {
      return;
    }
    // gtag expects an arguments object, not an array.
    (function () {
      window.dataLayer.push(arguments);
    })("event", name, params);
  }

  // Code block copy buttons: record the first line of what was copied.
  function snippet(button) {
    var target = document.querySelector(button.getAttribute("data-clipboard-target"));
    if (!target) {
      return "";
    }
    var line = target.textContent.split("\n").find(function (l) {
      return l.trim() !== "";
    }) || "";
    return line.trim().slice(0, 100);
  }

  document.addEventListener("click", function (event) {
    var copy = event.target.closest("[data-clipboard-target]");
    if (copy) {
      track("code_copy", { snippet: snippet(copy) });
      return;
    }

    // Landing page elements declare their event: data-omb-event plus optional
    // data-omb-target and data-omb-driver parameters.
    var el = event.target.closest("[data-omb-event]");
    if (el) {
      var params = {};
      if (el.dataset.ombTarget) {
        params.target = el.dataset.ombTarget;
      }
      if (el.dataset.ombDriver) {
        params.driver = el.dataset.ombDriver;
      }
      track(el.dataset.ombEvent, params);
    }
  });
})();
