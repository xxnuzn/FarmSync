/*
FarmSync 클라이언트 동기화 코어 (브라우저·Node 양쪽에서 동작).

이 파일이 이 프로젝트의 알맹이다. 전송 계층(TCP/WebSocket)이 절대 해주지 않는
**"연결이 끊긴 동안의 대화"** 를 애플리케이션 레벨에서 보장한다.

  - 로컬 큐(Outbox)      : 못 보낸 메시지를 순서대로 보관        (L7 응용)
  - 수신 오프셋(last_seq) : 어디까지 받았는지 기억              (L5 세션)
  - 멱등성 키(cid)        : 재전송해도 중복이 생기지 않게        (L7 응용)
  - 전역 순번 병합        : 서버가 준 seq 로 순서를 확정         (L5 세션)
*/
(function (root, factory) {
  if (typeof module === "object" && module.exports) {
    module.exports = factory();
  } else {
    root.FarmSyncCore = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var STATES = {
    OFFLINE: "offline",
    CONNECTING: "connecting",
    SYNCING: "syncing",
    ONLINE: "online",
  };

  var STATE_LABEL = {
    offline: "오프라인",
    connecting: "연결 중",
    syncing: "재동기화 중",
    online: "온라인",
  };

  function uid() {
    if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
    return "cid-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 10);
  }

  function createCore(options) {
    var opts = options || {};
    var storage = opts.storage;
    var transport = opts.transport; // { send(obj), close() }
    var onEvent = opts.onEvent || function () {};
    var clock = opts.now || function () { return Date.now(); };
    var room = opts.room || "default";
    var user = opts.user || "guest";
    var clientId = opts.clientId || uid();
    var inboxLimit = opts.inboxLimit || 300;

    var state = STATES.OFFLINE;
    var outbox = [];   // {cid, text, ts, status:'queued'|'inflight'|'sent', seq, duplicate}
    var inbox = [];    // {seq, cid, from, text, ts, read_by}
    var lastSeq = 0;   // 수신 오프셋 (L5)
    var sessionId = null;
    var serverLatest = 0;
    var counters = {
      queued: 0,
      sent: 0,
      received: 0,
      duplicatesIgnored: 0,
      resyncs: 0,
      resyncMessages: 0,
      orderViolations: 0,
    };

    // ------------------------------------------------------------- 상태 저장
    function snapshot() {
      return {
        state: state,
        stateLabel: STATE_LABEL[state] || state,
        outbox: outbox,
        inbox: inbox,
        lastSeq: lastSeq,
        sessionId: sessionId,
        serverLatest: serverLatest,
        counters: counters,
        user: user,
        room: room,
        clientId: clientId,
        pending: outbox.filter(function (m) { return m.status !== "sent"; }).length,
        unread: inbox.filter(function (m) { return m.from !== user && !(m.read_by || []).length; }).length,
      };
    }

    function emit(kind, extra) {
      var payload = snapshot();
      payload.kind = kind;
      if (extra) {
        for (var k in extra) if (Object.prototype.hasOwnProperty.call(extra, k)) payload[k] = extra[k];
      }
      onEvent(payload);
    }

    function persist() {
      if (!storage || !storage.set) return Promise.resolve();
      return storage.set("farmsync.state." + room + "." + user, {
        outbox: outbox,
        inbox: inbox,
        lastSeq: lastSeq,
        clientId: clientId,
      }).catch(function () {});
    }

    function load() {
      if (!storage || !storage.get) return Promise.resolve(false);
      return storage.get("farmsync.state." + room + "." + user).then(function (saved) {
        if (!saved) return false;
        outbox = saved.outbox || [];
        inbox = saved.inbox || [];
        lastSeq = saved.lastSeq || 0;
        if (saved.clientId) clientId = saved.clientId;
        // 이전에 전송 중이던 메시지는 상태가 불확실하므로 큐로 되돌린다.
        outbox.forEach(function (m) {
          if (m.status === "inflight") m.status = "queued";
        });
        return true;
      }).catch(function () { return false; });
    }

    function setState(next) {
      if (state === next) return;
      state = next;
      emit("state");
    }

    // ------------------------------------------------------------- 병합 (L5)
    function mergeMessages(list) {
      var added = 0;
      var maxSeq = lastSeq;
      (list || []).forEach(function (m) {
        if (typeof m.seq !== "number") return;
        var prev = null;
        inbox.some(function (x) { if (x.seq === m.seq) { prev = x; return true; } return false; });
        if (prev) {
          // 같은 메시지를 다시 받으면 최신 내용으로 덮어쓴다.
          // (공동 일정 초대 카드의 수락 여부가 서버 기준으로 갱신되도록)
          if (m.kind) prev.kind = m.kind;
          if (m.ref != null) prev.ref = m.ref;
          if (m.invite) prev.invite = m.invite;
          if (m.read_by) prev.read_by = m.read_by;
          if (m.read_total != null) prev.read_total = m.read_total;
        } else {
          var rec = {
            seq: m.seq, cid: m.cid, from: m.from, text: m.text, ts: m.ts,
            read_by: m.read_by || [],
          };
          if (m.kind) rec.kind = m.kind;
          if (m.ref != null) rec.ref = m.ref;
          if (m.invite) rec.invite = m.invite;
          if (m.read_total != null) rec.read_total = m.read_total;
          inbox.push(rec);
          added++;
          counters.received++;
        }
        if (m.seq > maxSeq) maxSeq = m.seq;
      });
      // 서버 순번 기준으로 정렬한다. 순서는 클라이언트가 아니라 서버가 정한다.
      var sorted = inbox.slice().sort(function (a, b) { return a.seq - b.seq; });
      if (inbox.length !== sorted.length || inbox.some(function (x, i) { return x.seq !== sorted[i].seq; })) {
        inbox = sorted;
      }
      if (maxSeq > lastSeq) lastSeq = maxSeq;
      if (inbox.length > inboxLimit) inbox = inbox.slice(inbox.length - inboxLimit);
      return added;
    }

    // ------------------------------------------------------------- HELLO
    function sendHello() {
      transport.send({
        type: "HELLO",
        conv: room,        // 서버 규약: 대화방 키는 conv 로 보낸다 (room 은 하위호환용)
        room: room,
        user: user,
        client_id: clientId,
        last_seq: lastSeq,
        ts: clock(),
      });
    }

    // ------------------------------------------------------------- 큐 비우기
    function flush() {
      if (state !== STATES.ONLINE && state !== STATES.SYNCING) return 0;
      var sent = 0;
      outbox.forEach(function (m) {
        if (m.status === "queued" || m.status === "failed") {
          m.status = "inflight";
          m.sentAt = clock();
          transport.send({ type: "SEND", cid: m.cid, text: m.text, ts: m.ts });
          sent++;
        }
      });
      if (sent) {
        persist();
        emit("flush", { flushed: sent });
      }
      return sent;
    }

    // ------------------------------------------------------------- 수신 처리
    function handle(message) {
      if (!message || !message.type) return;
      switch (message.type) {
        case "WELCOME":
          sessionId = message.session_id || message.conv || null;
          serverLatest = message.latest_seq || 0;
          emit("welcome", { online: message.online_ids || message.online || [] });
          // 서버가 보낼 밀린 메시지가 없으면 곧바로 온라인으로 전환하고 큐를 내보낸다.
          // 밀린 메시지가 있으면 SYNC 를 받아 병합한 뒤에 내보낸다 (순서가 뒤섞이지 않게).
          if (serverLatest <= lastSeq) {
            setState(STATES.ONLINE);
            persist();
            flush();
          }
          break;

        case "SYNC": {
          var added = mergeMessages(message.messages);
          counters.resyncs++;
          counters.resyncMessages += added;
          emit("sync", {
            fromSeq: message.from_seq,
            toSeq: message.to_seq,
            count: message.count,
            added: added,
          });
          // 재동기화가 끝난 뒤에야 큐를 비운다. 그래야 순서가 뒤섞이지 않는다.
          setState(STATES.ONLINE);
          persist();
          flush();
          break;
        }

        case "MSG": {
          var before = inbox.length;
          mergeMessages([message]);
          if (inbox.length > before) emit("message", { message: message });
          persist();
          break;
        }

        case "DELIVERED": {
          var found = null;
          outbox.forEach(function (m) {
            if (m.cid === message.cid) found = m;
          });
          if (found) {
            found.seq = message.seq;
            found.read_total = message.read_total || 0;
            // 내가 보낸 메시지도 수신함에 넣는다 — 그래야 읽음 수와 시간을 함께 그릴 수 있다.
            if (!message.duplicate) {
              var own = {
                seq: message.seq, cid: message.cid, from: user,
                text: found.text, ts: found.ts, read_by: [],
                read_total: message.read_total || 0,
              };
              var dup = inbox.some(function (x) { return x.seq === own.seq; });
              if (!dup) {
                inbox.push(own);
                inbox.sort(function (a, b) { return a.seq - b.seq; });
                counters.received++;
                if (own.seq > lastSeq) lastSeq = own.seq;
              }
            }
            if (message.duplicate) {
              // 서버가 이미 갖고 있었다 → 중복 저장되지 않음
              found.status = "sent";
              found.duplicate = true;
              counters.duplicatesIgnored++;
              emit("duplicate", { cid: message.cid, seq: message.seq });
            } else {
              found.status = "sent";
              found.duplicate = false;
              counters.sent++;
              emit("delivered", { cid: message.cid, seq: message.seq });
            }
          }
          persist();
          break;
        }

        case "READ": {
          var target = null;
          inbox.forEach(function (m) { if (m.seq === message.seq) target = m; });
          if (target) {
            target.read_by = (target.read_by || []).concat([message.reader]);
            emit("read", { seq: message.seq, reader: message.reader });
          }
          persist();
          break;
        }

        case "PRESENCE":
          // 서버 규약: user_id / online / online_ids
          emit("presence", {
            user: message.user_id,
            online: message.online,
            users: message.online_ids || [],
          });
          break;

        case "PONG":
          // 왕복 시간 — 네트워크 계층(L3)의 실시간 지표
          emit("pong", { rtt: Math.max(0, clock() - (message.ts || 0)) });
          break;

        case "EVENT":
          emit("layer", { layer: message.layer, layerName: message.layer_name, text: message.text });
          break;

        case "ERROR":
          // 전송이 거부되면 그 메시지를 '전달 실패' 로 표시한다 (cid 가 실려 오는 경우)
          if (message.cid) {
            outbox.forEach(function (m) {
              if (m.cid === message.cid) { m.status = "failed"; m.reason = message.detail; }
            });
            emit("failed", { cid: message.cid, detail: message.detail });
          }
          emit("error", { code: message.code, detail: message.detail });
          break;

        default:
          emit("unknown", { message: message });
      }
    }

    // ------------------------------------------------------------- 공개 API
    return {
      STATES: STATES,
      STATE_LABEL: STATE_LABEL,
      snapshot: snapshot,
      load: load,
      persist: persist,

      connect: function () {
        setState(STATES.CONNECTING);
        transport.open();
      },

      // 전송 계층이 열리면 HELLO 로 수신 오프셋을 알린다.
      onTransportOpen: function () {
        setState(STATES.SYNCING);
        // 이전 연결에서 배달 확인을 못 받은 메시지는 다시 보낼 수 있도록 큐로 되돌린다.
        outbox.forEach(function (m) { if (m.status === "inflight") m.status = "queued"; });
        sendHello();
      },

      onTransportClose: function () {
        setState(STATES.OFFLINE);
      },

      handle: handle,

      /** 사용자가 보내기 — 온라인이든 아니든 항상 큐에 먼저 넣는다. */
      submit: function (text) {
        if (!text || !text.trim()) return null;
        var item = { cid: uid(), text: text, ts: clock(), status: "queued", seq: null };
        outbox.push(item);
        counters.queued++;
        emit("queued", { item: item });
        persist();
        flush();
        return item;
      },

      /** 서버 이력(과거 대화)을 수신함에 채워 넣는다 — 새로 열어도 이전 대화가 보이게. */
      seed: function (list) {
        var added = mergeMessages(list);
        persist();
        emit("seed", { added: added, total: inbox.length });
        return added;
      },

      /** 보내지 못하고 남은 메시지를 다시 시도한다. */
      retry: function () {
        outbox.forEach(function (m) {
          if (m.status === "inflight") m.status = "queued";
        });
        return flush();
      },

      /** 오래 매달린 전송을 '전달 실패' 로 확정한다 (연결이 반쯤 죽었을 때). */
      sweep: function (ms) {
        var now = clock(), limit = ms || 12000, n = 0;
        outbox.forEach(function (m) {
          if (m.status === "inflight" && m.sentAt && now - m.sentAt > limit) {
            m.status = "failed"; m.reason = "서버 응답 없음"; n++;
          }
        });
        if (n) { emit("failed", { count: n }); persist(); }
        return n;
      },

      /** 네트워크 계층 왕복 시간 측정용 (서버가 같은 ts 를 되돌려준다). */
      ping: function () {
        transport.send({ type: "PING", ts: clock() });
      },

      markRead: function (seq) {
        // 연결이 아직 안 열렸으면 false 를 돌려준다 — 호출한 쪽이 나중에 다시 시도한다.
        return transport.send({ type: "READ", seq: seq });
      },

      /** 데모용: 연결을 강제로 끊는다 (실제 단절과 같은 경로를 탄다). */
      forceOffline: function () {
        transport.close(true);
        setState(STATES.OFFLINE);
      },

      /** 저장된 로컬 상태 초기화. */
      reset: function () {
        outbox = [];
        inbox = [];
        lastSeq = 0;
        counters = {
          queued: 0, sent: 0, received: 0, duplicatesIgnored: 0,
          resyncs: 0, resyncMessages: 0, orderViolations: 0,
        };
        emit("reset");
        return persist();
      },
    };
  }

  return { createCore: createCore, STATES: STATES, STATE_LABEL: STATE_LABEL, uid: uid };
});
