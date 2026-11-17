#!/usr/bin/env python3
"""
j8b ajanı — ev tarafı. HA'ya public WebSocket API ile bağlanır, broker'a docs/MQTT-SOZLESME.md sözleşmesiyle konuşur.
Aynı kod: (1) Mac'te geliştirme, (2) HA add-on konteyneri, (3) bağımsız Docker. Fark yalnız ortam değişkenleri.

Ortam:
  HA_URL        http://192.168.1.225:8123        (add-on'da http://supervisor/core)
  HA_TOKEN      uzun ömürlü token                 (add-on'da SUPERVISOR_TOKEN)
  J8B_API       https://api.j8b.net              (lab: http://127.0.0.1:8095)
  PAIR_CODE     ilk çalıştırmada eşleştirme kodu; sonrası STATE_FILE'dan okunur
  STATE_FILE    ./.home.json (uuid + mqtt kimliği)  — add-on'da /data/home.json
  INSTALL_TYPE  os | container | core | dev
İlke: config gelmeden hiçbir state yayınlanmaz; yalnız expose listesindekiler çıkar; attributes'tan kayıt defterinde olanlar atılır.
0.2.1 (M8): eylem {notify:"metin"} → event/notify (bulut bildirime çevirir)
0.2.0 (M7): config.automations[] → EVDE çalışan kural motoru (tetik: to|above|below + for_sec; koşul: saat/gün/durum; eylem: call_service);
            cmd {actions:[…]} → sahne (sırayla uygula, tek ack); event/automation → bulutta çalışma kaydı.
"""
import asyncio, json, os, sys, time, signal, ssl, logging
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None
import aiohttp
import paho.mqtt.client as mqtt

AGENT_VERSION = "0.3.1"
log = logging.getLogger("j8b")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

# ---- ortam: add-on icinde (SUPERVISOR_TOKEN var) varsayilanlar Supervisor'a gore; options.json okunur
_OPTS = {}
try:
    with open("/data/options.json") as _f: _OPTS = json.load(_f)
except Exception: pass
_SUP = os.environ.get("SUPERVISOR_TOKEN", "")
HA_URL     = os.environ.get("HA_URL") or ("http://supervisor/core" if _SUP else "http://127.0.0.1:8123")
HA_URL     = HA_URL.rstrip("/")
HA_TOKEN   = os.environ.get("HA_TOKEN") or _SUP
J8B_API    = (os.environ.get("J8B_API") or _OPTS.get("api_url") or "https://api.j8b.net").rstrip("/")
PAIR_CODE  = os.environ.get("PAIR_CODE") or str(_OPTS.get("pair_code") or "").strip().upper()
STATE_FILE = os.environ.get("STATE_FILE") or ("/data/home.json" if os.path.isdir("/data") else os.path.join(os.path.dirname(os.path.abspath(__file__)), ".home.json"))
INSTALL    = os.environ.get("INSTALL_TYPE") or ("os" if _SUP else "dev")
HAOS_VERSION = ""
STRIP_ATTR = {"friendly_name", "icon", "supported_features", "attribution", "entity_picture"}


# ---------------------------------------------------------------- eşleştirme
async def supervisor_info(session):
    """Add-on icinde Supervisor /info: HAOS surumu ve kurulum tipi (yoksa sessizce bos)."""
    global HAOS_VERSION, INSTALL
    if not _SUP: return
    try:
        async with session.get("http://supervisor/info", headers={"Authorization": "Bearer " + _SUP}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = (await r.json()).get("data", {})
            HAOS_VERSION = d.get("hassos") or ""
            INSTALL = "os" if d.get("hassos") else "supervised"
    except Exception as e:
        log.debug("supervisor info yok: %s", e)

async def ensure_paired(session):
    """STATE_FILE varsa onu kullan; yoksa PAIR_CODE ile /v1/agent/pair → kaydet. Broker parolası yalnız bu dosyada."""
    st = None
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            st = json.load(f)
        if not PAIR_CODE or st.get("code") == PAIR_CODE:
            return st
        log.info("Yeni eşleştirme kodu verildi (%s…) — ev yeniden eşleştiriliyor", PAIR_CODE[:3])
    if not PAIR_CODE:
        log.error("Eşleşme yok: PAIR_CODE verin (web uygulamasında Ev Ekle → kod)."); sys.exit(2)
    ha_ver = ""
    try:
        async with session.get(HA_URL + "/api/config", headers={"Authorization": "Bearer " + HA_TOKEN}) as r:
            ha_ver = (await r.json()).get("version", "")
    except Exception as e:
        log.warning("HA sürümü okunamadı: %s", e)
    body = {"code": PAIR_CODE, "agent": AGENT_VERSION, "ha": ha_ver, "haos": HAOS_VERSION, "install": INSTALL}
    async with session.post(J8B_API + "/v1/agent/pair", json=body) as r:
        j = await r.json()
    if not j.get("status"):
        log.error("Eşleşme reddedildi: %s", j.get("error")); sys.exit(3)
    st = {"uuid": j["data"]["home"]["uuid"], "name": j["data"]["home"]["name"], "mqtt": j["data"]["mqtt"], "paired_at": int(time.time()), "code": PAIR_CODE}
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)
    os.chmod(STATE_FILE, 0o600)
    log.info("Eşleşti: ev %s (%s)", st["name"], st["uuid"])
    return st


# ---------------------------------------------------------------- HA WebSocket istemcisi
class HA:
    def __init__(self, session):
        self.s = session; self.ws = None; self.mid = 0; self.pending = {}; self.on_event = None

    async def connect(self):
        url = HA_URL.replace("http", "ws", 1) + "/api/websocket"
        self.ws = await self.s.ws_connect(url, heartbeat=30)
        first = await self.ws.receive_json()
        if first.get("type") != "auth_required": raise RuntimeError("auth_required bekleniyordu: %r" % first)
        await self.ws.send_json({"type": "auth", "access_token": HA_TOKEN})
        ok = await self.ws.receive_json()
        if ok.get("type") != "auth_ok": raise RuntimeError("HA auth başarısız: %r" % ok)
        log.info("HA bağlı · %s", ok.get("ha_version"))
        return ok.get("ha_version", "")

    async def call(self, msg, timeout=20):
        self.mid += 1; msg = dict(msg, id=self.mid)
        fut = asyncio.get_event_loop().create_future(); self.pending[self.mid] = fut
        await self.ws.send_json(msg)
        return await asyncio.wait_for(fut, timeout)

    async def reader(self):
        async for m in self.ws:
            if m.type != aiohttp.WSMsgType.TEXT: break
            d = json.loads(m.data)
            if d.get("type") == "result":
                fut = self.pending.pop(d["id"], None)
                if fut and not fut.done():
                    if d.get("success"): fut.set_result(d.get("result"))
                    else: fut.set_exception(RuntimeError(str(d.get("error"))))
            elif d.get("type") == "event" and self.on_event:
                await self.on_event(d["event"])
        raise ConnectionError("HA websocket kapandı")


# ---------------------------------------------------------------- ajan
class Agent:
    def __init__(self, st):
        self.st = st; self.uuid = st["uuid"]; self.root = "j8b/" + self.uuid
        self.expose = set(); self.expose_all = False; self.config_seen = False
        self.rules = []; self.rule_tasks = {}; self.rule_last = {}; self._states = {}; self.tz = None      # M7 kural motoru; tz = HA'nın saat dilimi
        self.registry = {}; self.iot_cache = {}; self.ha = None; self.ha_version = ""
        self.loop = None; self.q = asyncio.Queue()
        m = st["mqtt"]
        self.mq = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=m["user"], protocol=mqtt.MQTTv311, clean_session=True)
        self.mq.username_pw_set(m["user"], m["pass"])
        if m.get("tls"): self.mq.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        self.mq.will_set(self.root + "/status", json.dumps({"online": False, "ts": int(time.time())}), qos=1, retain=True)
        self.mq.on_connect = self._on_connect; self.mq.on_message = self._on_message; self.mq.on_disconnect = self._on_disconnect

    # --- MQTT (paho thread) → asyncio kuyruğu
    def _on_connect(self, c, u, flags, rc, props=None):
        failed = getattr(rc, "is_failure", None)
        if failed is None: failed = (int(rc) != 0) if isinstance(rc, int) else False
        if failed:
            self.auth_fail = getattr(self, "auth_fail", 0) + 1
            log.warning("MQTT reddetti (rc=%s) %d/3", rc, self.auth_fail)
            if self.auth_fail >= 3:
                self.loop.call_soon_threadsafe(self.q.put_nowait, ("revoked", str(rc)))
            return
        self.auth_fail = 0
        log.info("MQTT bağlı (rc=%s)", rc)
        c.subscribe([(self.root + "/config", 1), (self.root + "/cmd/#", 1)])
        self.loop.call_soon_threadsafe(self.q.put_nowait, ("connected", None))
    def _on_disconnect(self, c, u, flags, rc, props=None):
        log.warning("MQTT koptu (rc=%s)", rc)
    def _on_message(self, c, u, msg):
        self.loop.call_soon_threadsafe(self.q.put_nowait, ("msg", (msg.topic, msg.payload)))

    def pub(self, sub, obj, qos=1, retain=False):
        self.mq.publish(self.root + "/" + sub, json.dumps(obj, ensure_ascii=False, separators=(",", ":")), qos=qos, retain=retain)

    # --- durum / kayıt defteri
    def publish_status(self, online=True):
        self.pub("status", {"online": online, "agent": AGENT_VERSION, "ha": self.ha_version, "haos": HAOS_VERSION, "install": INSTALL, "ts": int(time.time())}, 1, True)

    async def iot_class(self, integration):
        if not integration: return ""
        if integration in self.iot_cache: return self.iot_cache[integration]
        try:
            mf = await self.ha.call({"type": "manifest/get", "integration": integration})
            self.iot_cache[integration] = mf.get("iot_class", "") or ""
        except Exception:
            self.iot_cache[integration] = ""
        return self.iot_cache[integration]

    async def sync_registry(self):
        areas   = await self.ha.call({"type": "config/area_registry/list"})
        devices = await self.ha.call({"type": "config/device_registry/list"})
        ents    = await self.ha.call({"type": "config/entity_registry/list"})
        states  = {s["entity_id"]: s for s in await self.ha.call({"type": "get_states"})}
        dev_area = {}; out_dev = []
        for d in devices:
            integ = ""
            for ident in d.get("identifiers") or []:
                if isinstance(ident, (list, tuple)) and ident: integ = str(ident[0]); break
            dev_area[d["id"]] = d.get("area_id") or ""
            out_dev.append({"device_id": d["id"], "name": d.get("name_by_user") or d.get("name") or "", "manufacturer": d.get("manufacturer") or "",
                            "model": d.get("model") or "", "area_id": d.get("area_id") or "", "integration": integ,
                            "iot_class": await self.iot_class(integ), "sw_version": d.get("sw_version") or ""})
        out_ent = []; self.registry = {}
        for e in ents:
            eid = e["entity_id"]; st = states.get(eid, {}); attr = st.get("attributes", {}) if st else {}
            item = {"entity_id": eid, "domain": eid.split(".", 1)[0], "device_class": e.get("device_class") or e.get("original_device_class") or attr.get("device_class", "") or "",
                    "name": e.get("name") or e.get("original_name") or attr.get("friendly_name", "") or eid, "device_id": e.get("device_id") or "",
                    "area_id": e.get("area_id") or dev_area.get(e.get("device_id") or "", ""), "unit": e.get("unit_of_measurement") or attr.get("unit_of_measurement", "") or "",
                    "supported_features": int(e.get("supported_features") or attr.get("supported_features") or 0), "icon": e.get("icon") or e.get("original_icon") or attr.get("icon", "") or "",
                    "disabled": bool(e.get("disabled_by")), "hidden": bool(e.get("hidden_by"))}
            out_ent.append(item); self.registry[eid] = item
        # kayıt defterinde olmayan ama state'i olan entity'ler (ör. otomatik üretilen) de listelensin
        for eid, st in states.items():
            if eid in self.registry: continue
            attr = st.get("attributes", {})
            item = {"entity_id": eid, "domain": eid.split(".", 1)[0], "device_class": attr.get("device_class", ""), "name": attr.get("friendly_name", eid), "device_id": "", "area_id": "",
                    "unit": attr.get("unit_of_measurement", ""), "supported_features": int(attr.get("supported_features") or 0), "icon": attr.get("icon", ""), "disabled": False, "hidden": False}
            out_ent.append(item); self.registry[eid] = item
        payload = {"areas": [{"area_id": a["area_id"], "name": a.get("name", ""), "icon": a.get("icon") or ""} for a in areas], "devices": out_dev, "entities": out_ent, "ts": int(time.time())}
        self.pub("registry", payload, 1, True)
        log.info("Kayıt defteri yayınlandı: %d alan, %d cihaz, %d entity", len(areas), len(out_dev), len(out_ent))
        self._states = states

    def exposed(self, eid): return self.expose_all or eid in self.expose

    def publish_state(self, eid, st):
        if not st or not self.exposed(eid): return
        a = {k: v for k, v in (st.get("attributes") or {}).items() if k not in STRIP_ATTR}
        self.pub("state/" + eid, {"s": st.get("state"), "a": a, "lc": _ts(st.get("last_changed")), "lu": _ts(st.get("last_updated"))}, 0, True)

    def publish_all_exposed(self):
        n = 0
        for eid, st in getattr(self, "_states", {}).items():
            if self.exposed(eid): self.publish_state(eid, st); n += 1
        log.info("Açığa çıkarılan %d entity'nin durumu yayınlandı", n)

    # --- gelen mesajlar
    async def handle_config(self, payload):
        try: cfg = json.loads(payload or b"{}")
        except Exception: cfg = {}
        old = set(self.expose); self.expose = set(cfg.get("expose") or []); self.expose_all = bool(cfg.get("expose_all"))
        self.config_seen = True
        for eid in old - self.expose:                      # artık açık olmayanın retained state'i temizlenir
            if not self.expose_all: self.mq.publish(self.root + "/state/" + eid, b"", qos=0, retain=True)
        self.rules = [r for r in (cfg.get("automations") or []) if isinstance(r, dict) and r.get("trigger") and r.get("actions")]
        for t in self.rule_tasks.values(): t.cancel()
        self.rule_tasks = {}
        log.info("Config alındı: %d entity açık (expose_all=%s), %d otomasyon", len(self.expose), self.expose_all, len(self.rules))
        self.publish_all_exposed()

    async def call_action(self, a):
        """Tek eylem {entity_id, service, data}: yalnız açığa çıkarılmış entity; domain entity_id'den türetilir."""
        eid = a["entity_id"]; domain = eid.split(".", 1)[0]
        if not self.exposed(eid): raise PermissionError("entity açığa çıkarılmamış: " + eid)
        if a.get("domain") and a["domain"] != domain: raise ValueError("domain uyuşmuyor")
        await self.ha.call({"type": "call_service", "domain": domain, "service": a["service"], "service_data": a.get("data") or {}, "target": {"entity_id": eid}})

    async def run_actions(self, actions):
        """Sahne/otomasyon eylemleri sırayla; hatalar toplanır, kalanlar yine denenir. Döner: hata listesi."""
        errs = []
        for a in actions:
            try:
                if "notify" in a:                                   # M8: bildirim eylemi → bulut (event/notify) → e-posta/push
                    self.pub("event/notify", {"message": str(a["notify"])[:200], "automation_id": a.get("_rid"), "name": a.get("_rname"), "ts": int(time.time())}, 1, False)
                    continue
                await self.call_action(a)
            except Exception as e: errs.append(a.get("entity_id", "?") + ": " + str(e)[:80])
        return errs

    async def handle_cmd(self, topic, payload):
        rid = topic.rsplit("/", 1)[-1]; ok = False; err = None
        try:
            c = json.loads(payload)
            if isinstance(c.get("actions"), list):                 # M7: sahne — çoklu eylem, tek ack
                errs = await self.run_actions(c["actions"]); ok = not errs; err = "; ".join(errs)[:200] or None
            else:
                await self.call_action(c); ok = True
        except Exception as e:
            err = str(e)[:200]
        self.pub("ack/" + rid, {"ok": ok, "error": err, "ts": int(time.time())}, 1, False)
        log.info("cmd %s → %s %s", rid[:8], "ok" if ok else "HATA", err or "")

    # --- M7 kural motoru (evde çalışır; bulut yalnız tanımı gönderir)
    def _num(self, v):
        try: return float(v)
        except Exception: return None

    def rule_fires(self, r, old, new):
        """Tetik eşleşti mi? to: durum tam o değere DEĞİŞTİ; above/below: eşik GEÇİLDİ (eski diğer tarafta)."""
        t = r["trigger"]; os_ = old.get("state") if old else None; ns = new.get("state")
        if "to" in t:
            if ns != t["to"] or os_ == ns: return False
            if t.get("from") and os_ != t["from"]: return False
            return True
        n = self._num(ns); o = self._num(os_)
        if n is None: return False
        if "above" in t: return n > float(t["above"]) and (o is None or o <= float(t["above"]))
        if "below" in t: return n < float(t["below"]) and (o is None or o >= float(t["below"]))
        return False

    def conditions_ok(self, r):
        c = r.get("conditions") or {}
        if not isinstance(c, dict): return True
        now = datetime.now(self.tz) if self.tz else datetime.now()
        hm = now.strftime("%H:%M"); dow = now.isoweekday()   # Pzt=1
        a, b = c.get("after"), c.get("before")
        if a and b:
            inside = (a <= hm < b) if a <= b else (hm >= a or hm < b)      # gece yarısını aşan aralık (18:00-06:00)
            if not inside: return False
        elif a and hm < a: return False
        elif b and hm >= b: return False
        if c.get("days") and dow not in c["days"]: return False
        for sc in c.get("states") or []:
            st = self._states.get(sc.get("entity_id"))
            if not st or str(st.get("state")) != str(sc.get("state")): return False
        return True

    async def fire_rule(self, r, delay=0):
        rid = r.get("id"); t = r["trigger"]
        try:
            if delay:
                await asyncio.sleep(delay)
                cur = self._states.get(t["entity_id"])               # süre dolunca hâlâ tetik durumunda mı?
                if not cur: return
                if "to" in t and cur.get("state") != t["to"]: return
                n = self._num(cur.get("state"))
                if "above" in t and (n is None or n <= float(t["above"])): return
                if "below" in t and (n is None or n >= float(t["below"])): return
            if not self.conditions_ok(r): log.info("otomasyon %s: koşul tutmadı", r.get("name")); return
            if time.time() - self.rule_last.get(rid, 0) < 2: return  # sektirme
            self.rule_last[rid] = time.time()
            errs = await self.run_actions([dict(a, _rid=rid, _rname=r.get("name")) if "notify" in a else a for a in r["actions"]])
            self.pub("event/automation", {"id": rid, "name": r.get("name"), "ok": not errs, "error": "; ".join(errs)[:200] or None, "ts": int(time.time())}, 1, False)
            log.info("otomasyon %s → %s %s", r.get("name"), "ok" if not errs else "HATA", "; ".join(errs))
        except asyncio.CancelledError:
            pass
        finally:
            self.rule_tasks.pop(rid, None)

    def eval_rules(self, eid, old, new):
        for r in self.rules:
            t = r["trigger"]
            if t.get("entity_id") != eid: continue
            if self.rule_fires(r, old, new):
                if r.get("id") in self.rule_tasks: continue
                self.rule_tasks[r.get("id")] = asyncio.ensure_future(self.fire_rule(r, int(t.get("for_sec") or 0)))
            elif "to" in t and new.get("state") != t["to"] and r.get("id") in self.rule_tasks:
                self.rule_tasks[r.get("id")].cancel()                 # bekleme sırasında durum bozuldu → iptal

    async def on_ha_event(self, ev):
        if ev.get("event_type") != "state_changed": return
        d = ev.get("data", {}); eid = d.get("entity_id"); new = d.get("new_state"); old = d.get("old_state") or self._states.get(eid)
        if new is None: return
        self._states[eid] = new
        if self.exposed(eid): self.publish_state(eid, new)
        if self.rules: self.eval_rules(eid, old, new)

    # --- ana döngü
    async def run(self):
        self.loop = asyncio.get_event_loop()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
            self.ha = HA(session); self.ha.on_event = self.on_ha_event
            self.ha_version = await self.ha.connect()
            reader = asyncio.create_task(self.ha.reader())
            m = self.st["mqtt"]
            self.mq.connect_async(m["host"], int(m["port"]), keepalive=60); self.mq.loop_start()
            await self.ha.call({"type": "subscribe_events", "event_type": "state_changed"})
            try:                                                   # HA'nın saat dilimi → otomasyon saat koşulları
                cfg = await self.ha.call({"type": "get_config"}); tzname = (cfg or {}).get("time_zone") or ""
                self.tz = ZoneInfo(tzname) if (tzname and ZoneInfo) else None
                log.info("Saat dilimi: %s", tzname or "sistem")
            except Exception as e:
                log.warning("Saat dilimi alınamadı (%s); sistem saati kullanılacak", e)
            last_status = 0
            while True:
                try:
                    kind, data = await asyncio.wait_for(self.q.get(), timeout=5)
                except asyncio.TimeoutError:
                    kind, data = None, None
                if reader.done(): reader.result()
                if kind == "revoked":
                    log.error("Ev bulutta silinmiş ya da kimliği iptal edilmiş (%s). Eşleşme dosyası kaldırıldı; j8b uygulamasından yeni kod alıp Yapılandırma'ya yazın.", data)
                    try: os.remove(STATE_FILE)
                    except Exception: pass
                    raise SystemExit(4)
                if kind == "connected":
                    self.publish_status(True); await self.sync_registry(); self.healthy = True
                elif kind == "msg":
                    topic, payload = data
                    if topic.endswith("/config"): await self.handle_config(payload)
                    elif "/cmd/" in topic: await self.handle_cmd(topic, payload)
                if time.time() - last_status >= 60:
                    self.publish_status(True); last_status = time.time()


def _ts(iso):
    if not iso: return None
    try:
        from datetime import datetime
        return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    except Exception: return None


async def main():
    """Üst döngü: HA soketi düşerse ya da bir hata olursa ajan ÖLMEZ — artan bekleme ile yeniden bağlanır.
    MQTT tarafını paho kendisi yeniden bağlar; HA tarafı burada. Son status LWT ile broker'da zaten 'offline' olur."""
    if not HA_TOKEN: log.error("HA_TOKEN yok"); sys.exit(2)
    async with aiohttp.ClientSession() as s:
        await supervisor_info(s)
        st = await ensure_paired(s)
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    backoff = 2
    while not stop.is_set():
        agent = Agent(st)
        task = asyncio.create_task(agent.run())
        stopper = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
        try:
            agent.publish_status(False)
        except Exception:
            pass
        try:
            agent.mq.loop_stop(); agent.mq.disconnect()
        except Exception:
            pass
        if stop.is_set():
            stopper.cancel(); task.cancel(); break
        stopper.cancel()
        err = task.exception() if task.done() else None
        if isinstance(err, SystemExit):
            raise err
        log.warning("Ajan döngüsü düştü (%s) — %d sn sonra yeniden bağlanılacak", err, backoff)
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass
        backoff = 2 if getattr(agent, 'healthy', False) else min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())
