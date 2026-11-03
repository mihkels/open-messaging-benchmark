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

// Replays the quickstart transcript in the landing page terminal. The full transcript is in the
// HTML, so without JavaScript, or with reduced motion, the final frame is shown as is.
(function () {
  "use strict";

  var body = document.querySelector("[data-omb-term]");
  if (!body) {
    return;
  }
  var replay = document.querySelector("[data-omb-replay]");
  var lines = Array.prototype.slice.call(body.querySelectorAll(".l"));
  var texts = lines.map(function (l) { return l.textContent; });
  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var timer = null;
  var caret = document.createElement("span");
  caret.className = "caret";

  function stop() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  }

  function showAll() {
    stop();
    lines.forEach(function (l, i) {
      l.hidden = false;
      l.textContent = texts[i];
    });
    body.appendChild(caret);
  }

  function scrollDown() {
    body.scrollTop = body.scrollHeight;
  }

  function play() {
    stop();
    lines.forEach(function (l) { l.hidden = true; });
    body.scrollTop = 0;
    var i = 0;

    function next() {
      if (i >= lines.length) {
        body.appendChild(caret);
        scrollDown();
        return;
      }
      var line = lines[i];
      var text = texts[i];
      var delay = parseInt(line.getAttribute("data-d") || "120", 10);
      line.hidden = false;
      if (line.classList.contains("cmd")) {
        var n = 0;
        line.textContent = "";
        line.appendChild(caret);
        (function type() {
          n += 1;
          line.textContent = text.slice(0, n);
          line.appendChild(caret);
          scrollDown();
          if (n < text.length) {
            timer = setTimeout(type, 22);
          } else {
            line.removeChild(caret);
            i += 1;
            timer = setTimeout(next, 450);
          }
        })();
      } else {
        line.textContent = text;
        scrollDown();
        i += 1;
        timer = setTimeout(next, delay);
      }
    }

    timer = setTimeout(next, 500);
  }

  if (replay) {
    replay.hidden = reduce;
    replay.addEventListener("click", play);
  }

  if (reduce) {
    showAll();
  } else {
    play();
  }
})();
