/* FarmSync 연결 복원 레이어 (v18) — 모든 페이지 공용.
   요청이 한 번 실패해도 화면이 영구히 멈추지 않게 한다:
   타임아웃 · GET 자동 재시도 · 401(세션 만료) 안내 · 연결 복구 시 자동 재요청 · 화면 오류 배너 · 연결 진단. */
(function () {
  "use strict";
  if (window.FS) return;

  /* v19 · 페이지가 없는 id 에 접근해 스크립트가 통째로 죽는 것을 막는다.
     숨김 자리표시자로 대체하고 콘솔에 원인 id 를 남긴다. */
  (function () {
    var orig = document.getElementById.bind(document);
    document.getElementById = function (id) {
      var el = orig(id);
      if (el) return el;
      window.__fsMissing = window.__fsMissing || {};
      if (!window.__fsMissing[id]) {
        window.__fsMissing[id] = 1;
        console.error("[farmsync] 없는 id 참조: #" + id + " → 숨김 자리표시자로 대체");
      }
      var ph = document.createElement("span");
      ph.hidden = true; ph.id = id;
      return ph;
    };
  })();

  var TIMEOUT = 9000;      // 요청 1회 제한 9초
  var TRIES = 3;           // GET 은 추가로 3번까지 재시도 (0.5s → 1s → 2s)
  var origFetch = window.fetch ? window.fetch.bind(window) : null;

  var bd = null, bt = null, ba = null, bpoll = null, kinds = {};

  function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

  function shell() {
    if (bd || !document.body) return bd;
    bd = document.createElement("div");
    bd.id = "fsBanner";
    bd.style.cssText = "position:fixed;left:0;right:0;top:0;z-index:2147483646;display:none;" +
      "font:14px/1.45 system-ui,'Apple SD Gothic Neo','Malgun Gothic',sans-serif;" +
      "background:#c4441a;color:#fff;padding:9px 14px;gap:10px;flex-wrap:wrap;" +
      "justify-content:center;align-items:center;box-shadow:0 2px 12px rgba(0,0,0,.28)";
    bt = document.createElement("span");
    bt.style.cssText = "font-weight:700";
    ba = document.createElement("button");
    ba.style.cssText = "border:0;border-radius:8px;padding:6px 13px;font:inherit;font-weight:700;" +
      "cursor:pointer;background:#fff;color:#c4441a";
    bpoll = document.createElement("button");
    bpoll.type = "button";
    bpoll.textContent = "연결 진단";
    bpoll.style.cssText = "border:1px solid rgba(255,255,255,.75);border-radius:8px;padding:6px 13px;" +
      "font:inherit;font-weight:600;cursor:pointer;background:transparent;color:#fff";
    bpoll.onclick = function () { diag(); };
    bd.appendChild(bt); bd.appendChild(ba); bd.appendChild(bpoll);
    document.body.appendChild(bd);
    return bd;
  }

  function show(kind, text, label, fn) {
    kinds = {}; kinds[kind] = 1;
    var el = shell();
    if (!el) return;
    bt.textContent = text;
    if (label) { ba.style.display = ""; ba.textContent = label; ba.onclick = fn; }
    else { ba.style.display = "none"; ba.onclick = null; }
    el.style.display = "flex";
  }

  function hide() { kinds = {}; if (bd) bd.style.display = "none"; }
  function visible() { return !!(bd && bd.style.display !== "none"); }

  function expired() {
    show("auth", "로그인이 만료되었습니다. 다시 로그인해 주세요.", "다시 로그인", function () {
      location.href = "/login?next=" + encodeURIComponent(location.pathname + location.search);
    });
  }

  function netFail(detail) {
    show("net", "서버에 연결하지 못했습니다" + (detail ? " · " + detail : "") + " — 자동으로 다시 시도합니다.", "다시 시도", function () { retry(); });
  }

  function srvFail(code) {
    show("srv", "서버가 응답하지 않습니다 (HTTP " + code + ").", "다시 시도", function () { retry(); });
  }

  /* 요청 1회: 9초 제한 + 캐시 금지 */
  function attempt(input, init) {
    var ctl = new AbortController();
    var timer = setTimeout(function () { ctl.abort(); }, TIMEOUT);
    var o = {}, k;
    for (k in (init || {})) { try { o[k] = init[k]; } catch (e) {} }
    o.signal = ctl.signal;
    o.cache = "no-store";
    return origFetch(input, o).then(function (r) { clearTimeout(timer); return r; },
      function (e) { clearTimeout(timer); throw e; });
  }

  function netResponse() {
    return new Response(JSON.stringify({ ok: false, code: "NETWORK", detail: "서버에 연결하지 못했습니다" }),
      { status: 599, statusText: "network-error", headers: { "Content-Type": "application/json" } });
  }

  /* 재시도 루프 — 401 은 재시도하지 않고 즉시 안내, 5xx/네트워크는 지수 백오프 */
  function run(input, init, tries, quiet) {
    var i = 0;
    function step() {
      return attempt(input, init).then(function (r) {
        if (r.status === 401) { if (!quiet) expired(); return r; }
        if (r.status >= 500) {
          if (i < tries) { i++; return sleep(500 * Math.pow(2, i - 1)).then(step); }
          if (!quiet) srvFail(r.status);
          return r;
        }
        if (!quiet) hide();
        return r;
      }, function (e) {
        if (i < tries) { i++; return sleep(500 * Math.pow(2, i - 1)).then(step); }
        if (!quiet) netFail(e && e.name === "AbortError" ? "응답 시간 초과 " + (TIMEOUT / 1000) + "초" : "네트워크 오류");
        return netResponse();
      });
    }
    return step();
  }

  if (origFetch) {
    window.fetch = function (input, init) {
      var url = (typeof input === "string") ? input : ((input && input.url) || "");
      if (url.indexOf("/api/") < 0) return origFetch(input, init);          // API 만 감싼다
      var method = ((init && init.method) || (input && input.method) || "GET").toUpperCase();
      return run(input, init, method === "GET" ? TRIES : 0, false);          // POST 는 중복 전송 방지를 위해 재시도하지 않는다
    };
  }

  /* 연결이 돌아오면 스스로 다시 불러온다 */
  var LOADERS = ["loadThreads", "loadEvents", "loadFriends", "refresh", "reload", "load", "bootstrap", "render", "seed", "sweep"];
  function retry() {
    hide();
    for (var i = 0; i < LOADERS.length; i++) {
      try { if (typeof window[LOADERS[i]] === "function") { window[LOADERS[i]](); return; } } catch (e) {}
    }
    location.reload();
  }
  window.addEventListener("fs:online", function () { if (visible()) retry(); });
  setInterval(function () { if (visible() && kinds.net) retry(); }, 20000);

  /* 화면 스크립트가 죽으면 조용히 빈 화면이 되지 않게 알린다 */
  window.addEventListener("error", function (ev) {
    if (ev && ev.message) show("crash", "화면 오류: " + String(ev.message).slice(0, 140), "새로 고침", function () { location.reload(); });
  });
  window.addEventListener("unhandledrejection", function (ev) {
    var r = ev && ev.reason, m = (r && (r.message || r)) || "알 수 없는 오류";
    show("crash", "처리되지 않은 오류: " + String(m).slice(0, 140), "새로 고침", function () { location.reload(); });
  });

  /* 연결 진단 — 실패했을 때 화면에서 바로 원인을 볼 수 있게 */
  function diag() {
    var box = document.createElement("div");
    box.style.cssText = "position:fixed;right:12px;bottom:12px;z-index:2147483647;max-width:min(600px,92vw);" +
      "background:#14261d;color:#e8f3ee;border-radius:12px;padding:14px 16px;" +
      "font:13px/1.65 ui-monospace,Menlo,Consolas,monospace;box-shadow:0 12px 34px rgba(0,0,0,.45)";
    var pre = document.createElement("pre");
    pre.style.cssText = "margin:0;white-space:pre-wrap;word-break:break-all";
    var close = document.createElement("button");
    close.type = "button"; close.textContent = "닫기";
    close.style.cssText = "margin-top:10px;border:0;border-radius:8px;padding:6px 13px;font:inherit;cursor:pointer;background:#17916a;color:#fff";
    close.onclick = function () { box.remove(); };
    box.appendChild(pre); box.appendChild(close);
    document.body.appendChild(box);

    var lines = ["FarmSync 연결 진단", "시각 " + new Date().toLocaleString("ko-KR"),
                 "주소 " + location.origin + location.pathname,
                 "쿠키 " + (document.cookie ? "있음" : "없음(세션 쿠키는 HttpOnly)"), ""];
    pre.textContent = lines.join("\n");
    var probes = ["/healthz", "/api/me", "/api/threads"];
    var i = 0;
    (function next() {
      if (i >= probes.length) { lines.push("", "→ 401이면 로그인 필요, 599/응답없음이면 연결 차단"); pre.textContent = lines.join("\n"); return; }
      var p = probes[i++], t0 = Date.now();
      run(p, {}, 1, true).then(function (r) {
        var ms = Date.now() - t0;
        var note = r.status === 401 ? "로그인 필요" : (r.status === 599 ? "연결 실패/시간 초과" : (r.status === 200 ? "정상" : "확인 필요"));
        lines.push(p + " → " + (r.status === 599 ? "응답 없음" : "HTTP " + r.status) + " · " + ms + "ms · " + note);
        pre.textContent = lines.join("\n");
        next();
      });
    })();
  }

  window.FS = { retry: retry, diag: diag, banner: show, hide: hide, online: retry, version:"22" };
})();
