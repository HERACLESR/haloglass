#!/usr/bin/env python3
"""halogen 只读监控仪表盘：采集 + 单页网页，仅标准库，不写任何文件。"""
# SPDX-License-Identifier: MIT
import gzip
import json
import math
import os
import threading
import time
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# S2a-P00r: 同目录 dash.config（KEY=VALUE，# 注释）先装载，真实环境变量优先；零第三方依赖（无 yaml/toml）
def _load_dash_config():
    p = Path(__file__).resolve().with_name("dash.config")
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for ln in lines:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, _, v = ln.partition("=")
            os.environ.setdefault(k.strip(), v.strip())


_load_dash_config()

HAL = os.environ.get("DASH_ENGINE_URL", "http://127.0.0.1:8899")  # w15-合规P1-3：引擎地址可配（默认回环 8899）
ENGINE_PROC = os.environ.get("DASH_ENGINE_PROC", "flash_serve")    # w15-合规P1-7：引擎进程名可配
INTERVAL = 1.0
HISTORY_LEN = 1800
METRICS_MISS_RESTART = 2   # metrics 连续 N 拍断供 = 判引擎重启（重启必伴 metrics 缺口）
DRM = Path("/sys/class/drm/card0/device")
PORTS = [8873, 8874, 8875, 8876, 8877, 8878, 8879]
# S2a-P01r (P1-3): Host 白名单 = 回环 + 已绑定地址（DASH_BINDS，默认 127.0.0.1）自动放行，代码零硬编码 IP/域名；
# 自定义域名/反代名用 DASH_ALLOWED_HOSTS（逗号分隔）扩展。挡 DNS rebinding。
# w15-bugP1-1: 通配绑定（0.0.0.0/::）或空绑定时【不做】自动 Host 放行（字面量 0.0.0.0 永远匹配不到真实
# Host，宣称"已绑定地址自动放行"对通配恰好不成立），启动打印 warning 引导设置 DASH_ALLOWED_HOSTS。
ALLOWED_HOSTS = {"localhost", "127.0.0.1", "::1"}
_BINDS_SET = {h.strip().lower() for h in os.environ.get("DASH_BINDS", "127.0.0.1").split(",") if h.strip()}
_WILDCARD_BIND = (not _BINDS_SET) or bool(_BINDS_SET & {"0.0.0.0", "::"})
if not _WILDCARD_BIND:
    ALLOWED_HOSTS |= _BINDS_SET
ALLOWED_HOSTS |= {h.strip().lower() for h in os.environ.get("DASH_ALLOWED_HOSTS", "").split(",") if h.strip()}

# history 只保留前端 sparkline/页脚真正消费的键（瘦身 data.json，latest 仍为全量）
HISTORY_KEYS = ("ts", "in_flight", "queued", "kv_cache_usage_ratio",
                "req_decode_ts", "cache_bytes", "mem_available_gib",
                "gpu_busy_percent", "cpu_temp_c", "gpu_temp_c",
                "ev_relocated", "ev_cold_resorts", "gpu_power_w",
                "gpu_mem_used_gib", "predicted_tokens_seconds",
                "decode_total_tps")

_lock = threading.Lock()
_cond = threading.Condition(_lock)   # 采集拍完成 → 唤醒所有 SSE 连接
_history = deque(maxlen=HISTORY_LEN)
_latest = {}
_tick = 0
_cycle = 0
_prev_pred = None   # 最近一次【有效】的 decode_tokens_total（缺口拍不覆盖，桥接任意间隙）
_metrics_miss = 0
_pending_gap = False  # 断供通道触发的"疑似重启"：metrics 恢复且无回退 = 误报，解除黑名单
_last_req = {}      # podman 采集已下线：恒空（重启检测逻辑原样保留，引用不清）
_req_fp = None      # 历史日志行指纹槽位（podman 下线后恒 None，重启检测逻辑原样引用）
_stale_fp = None    # 疑似（断供通道）黑名单（同上，恒 None）
_confirmed_fp = None  # 确认（回退通道）黑名单（同上，恒 None）
_speed_stale = None   # None | "restarted"（counter 回退确认）| "metrics_gap"（仅断供疑似）
_decode_win = deque(maxlen=120)   # w10-ⓕ：(墙钟ts, decode_tokens_total) 60s 滑窗（G2 调研：总解码吞吐）；w15-bugP0-1：maxlen=120（≥DECODE_WIN_S 秒 @1拍/秒）硬上限，⛔ 无限增长
DECODE_WIN_S = 60.0
_last_speed = None      # w11-ⓕ2：最近一次引擎上报的单流速度（predicted_tokens_seconds 仅在请求完成时非零）
DECODE_TPS_CAP = float(os.environ.get("DASH_DECODE_CAP", "72"))  # w13-ⓐ：本机带宽物理上限封顶（~70 tok/s，带宽共享不可加，in_flight×单流速乘积可虚高至 134）；w15-合规P1-2：DASH_DECODE_CAP 可配，前端经 data.json decode_cap 取同值
_slow = {}            # 慢通道快照（health/props），由 slow_collector 线程刷新


def fetch_retry(fn, retry_timeout=0.5):
    """采集通道取数：失败立即用【短超时】重试一次（P2-1：首试走通道自身超时，
    重试仅 0.5s —— 引擎正常时零开销，引擎偶发慢响应时单通道尾部 ≤2.5s），
    仍失败抛出最后一次异常（判异常前先重试）。fn(None)=用通道自身超时。"""
    try:
        return fn(None)
    except Exception:
        return fn(retry_timeout)


def proc_rss_kib():
    """返回 (全部进程 RSS 合计 KiB, flash_serve 进程 RSS 合计 KiB)；
    找不到 flash_serve 进程返回 (None, None)（前端显骨架）。"""
    tot = 0
    eng = 0
    found = False
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open("/proc/%s/cmdline" % d, "rb") as f:
                cmd = f.read(2048)
            rss = 0
            with open("/proc/%s/status" % d) as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss = int(line.split()[1])
                        break
        except (OSError, ValueError):
            continue
        tot += rss
        if ENGINE_PROC.encode() in cmd:  # w15-合规P1-7：进程名可配（默认 flash_serve）
            eng += rss
            found = True
    return (tot, eng) if found else (None, None)


def http_get(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "halogen-dash/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def read_text(path):
    try:
        return Path(path).read_text().strip()
    except Exception:
        return None


def to_int(s):
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def parse_metrics(text):
    d = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                v = float(parts[1])
            except ValueError:
                pass
            else:
                # w15-bugP0-1：NaN/Inf（Prometheus 除零等场景）一律丢弃，⛔ 入窗口毒化差分
                if math.isfinite(v):
                    d[parts[0]] = v
    return d


_hwmon_cache = {}


def hwmon_temps():
    out = {}
    for name in ("k10temp", "amdgpu"):
        p = _hwmon_cache.get(name)
        if p:
            v = to_int(read_text(p))
            if v is not None:
                out[name] = round(v / 1000.0, 1)
            else:
                _hwmon_cache.pop(name, None)
    if "k10temp" not in _hwmon_cache or "amdgpu" not in _hwmon_cache:
        # w15-bugP1-2：重扫按【温度键】判断（power 键由 gpu_power_w 自行判断），
        # ⛔ len(_hwmon_cache) 混合计数——power 在缓存时会把 len 撑到 ≥2 使温度键失效后永不再扫描
        try:
            for h in sorted(Path("/sys/class/hwmon").glob("hwmon*")):
                try:
                    nm = (h / "name").read_text().strip()
                except Exception:
                    continue
                if nm in ("k10temp", "amdgpu") and nm not in _hwmon_cache:
                    for t in sorted(h.glob("temp*_input")):
                        _hwmon_cache[nm] = str(t)
                        break
        except Exception:
            pass
    for name in ("k10temp", "amdgpu"):
        if name not in out:
            p = _hwmon_cache.get(name)
            if p:
                v = to_int(read_text(p))
                if v is not None:
                    out[name] = round(v / 1000.0, 1)
    return out


def gpu_power_w():
    """GPU 封装功率（µW→W），读不到返回 None。"""
    p = _hwmon_cache.get("power")
    if p:
        v = to_int(read_text(p))
        if v is not None:
            return round(v / 1e6, 1)
        _hwmon_cache.pop("power", None)
    try:
        for f in sorted(DRM.glob("hwmon/hwmon*/power1_average")):
            v = to_int(read_text(f))
            if v is not None:
                _hwmon_cache["power"] = str(f)
                return round(v / 1e6, 1)
    except Exception:
        pass
    return None


def slow_once():
    """慢通道：/health（引擎活性探测缓存过期时偶发 ~1s，技术调研 §2.1）+ /props。
    独立线程跑，结果缓存进 _slow 由 collect_once 消费 ——
    1s 采集节拍不再被 /health 探测拖慢（探测代价由本线程吸收）。
    通道超时/失败立即重试一次，仍失败才记 err_*。"""
    snap = {}
    try:
        h = json.loads(fetch_retry(lambda t: http_get(HAL + "/health", timeout=t or 2)))
        snap["halogen_ok"] = h.get("status") == "ok"
        for k in ("slots", "in_flight", "queued", "busy_for_s",
                  "longest_wait_s", "kv_pool_positions"):
            snap[k] = h.get(k)
        snap["model"] = h.get("model")
        snap["model_context"] = h.get("context")
        snap["kv_cache_type"] = h.get("kv_cache")
        snap["checkpoint_format"] = h.get("checkpoint_format")
        ver = h.get("version")
        snap["engine_version"] = ver.get("engine") if isinstance(ver, dict) else None
    except Exception as e:
        snap["halogen_ok"] = False
        snap["err_health"] = repr(e)[:100]
    try:
        p = json.loads(fetch_retry(lambda t: http_get(HAL + "/props", timeout=t or 3)))
        snap["model_alias"] = p.get("model_alias")
        snap["total_slots"] = p.get("total_slots")
        snap["engine_sleeping"] = bool(p.get("is_sleeping"))
    except Exception as e:
        snap["err_props"] = repr(e)[:100]
    with _lock:
        _slow.clear()
        _slow.update(snap)
        _cond.notify_all()


def collect_once():
    global _cycle, _prev_pred, _metrics_miss, _pending_gap, _req_fp, _stale_fp
    global _confirmed_fp, _speed_stale, _tick, _last_speed
    _cycle += 1
    snap = {"ts": round(time.time(), 1)}
    with _lock:
        snap.update(_slow)  # health/props 字段 = 慢线程最新快照（≤1s 旧）
    try:
        m = parse_metrics(fetch_retry(lambda t: http_get(HAL + "/metrics", timeout=t or 2)))
        snap["kv_pool_reserved_tokens"] = m.get("halogen:kv_pool_reserved_tokens")
        snap["kv_pool_waiting_for_room"] = m.get("halogen:kv_pool_waiting_for_room")
        snap["cache_bytes"] = m.get("halogen:cache_bytes")
        snap["kv_cache_usage_ratio"] = m.get("llamacpp:kv_cache_usage_ratio")
        snap["kv_cache_tokens"] = m.get("llamacpp:kv_cache_tokens")
        snap["queue_waited_total"] = m.get("halogen:queue_waited_total")
        snap["queue_wait_seconds_total"] = m.get("halogen:queue_wait_seconds_total")
        snap["queue_gave_up_total"] = m.get("halogen:queue_gave_up_total")
        snap["decode_tokens_total"] = m.get("llamacpp:tokens_predicted_total")
        snap["predicted_tokens_seconds"] = m.get("llamacpp:predicted_tokens_seconds")
        snap["prompt_tokens_seconds"] = m.get("llamacpp:prompt_tokens_seconds")
        snap["requests_total"] = m.get("halogen:requests_total")
        snap["engine_up"] = m.get("halogen:engine_up")
        dt = m.get("halogen:draft_tokens_total")
        da = m.get("halogen:draft_tokens_accepted_total")
        snap["draft_accept_rate"] = (da / dt) if (dt and da is not None) else None
    except Exception as e:
        snap["err_metrics"] = repr(e)[:100]
    try:
        c = json.loads(fetch_retry(lambda t: http_get(HAL + "/cache", timeout=t or 2)))
        pool = c.get("pool") if isinstance(c.get("pool"), dict) else {}
        snap["cache_waiting_for_room"] = pool.get("waiting_for_room")
        snap["cache_waiting_s"] = pool.get("waiting_s")
        snap["cache_relocated"] = pool.get("relocated")
        snap["cache_cold_resorts"] = pool.get("cold_resorts")
        snap["cache_held_regions"] = pool.get("held_regions")
        snap["cache_packed"] = pool.get("packed")
        snap["cache_room_clamped"] = pool.get("room_clamped")
        snap["cache_pool_usage_ratio"] = pool.get("usage_ratio")
        snap["cache_hit_rate"] = c.get("hit_rate")
        snap["cache_token_hit_rate"] = c.get("token_hit_rate")
        snap["cache_entries"] = c.get("entries")
        snap["cache_max_entries"] = c.get("max_entries")
        snap["cache_superseded"] = c.get("superseded")
        if snap.get("cache_bytes") is None:
            snap["cache_bytes"] = c.get("bytes")
    except Exception as e:
        snap["err_cache"] = repr(e)[:100]
    try:
        mem = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            k, _, v = line.partition(":")
            f = v.split()
            if f:
                mem[k] = int(f[0])
        total = mem.get("MemTotal")
        if total:
            gib = 1048576
            free = mem.get("MemFree", 0)
            bc = mem.get("Buffers", 0) + mem.get("Cached", 0)
            used = max(total - free - bc, 0)
            snap["mem_total_gib"] = round(total / gib, 1)
            snap["mem_used_gib"] = round(used / gib, 1)
            snap["mem_buffcache_gib"] = round(bc / gib, 1)
            snap["mem_free_gib"] = round(free / gib, 1)
            snap["mem_available_gib"] = round(mem.get("MemAvailable", 0) / gib, 1)
            # 内存全景 6 段（按占用者口径，和 = MemTotal；flash_serve 读不到则整卡不出键=前端骨架）
            tot_kib, eng_kib = proc_rss_kib()
            if tot_kib is not None:
                kernel = (mem.get("Slab", 0) + mem.get("KernelStack", 0)
                          + mem.get("PageTables", 0))
                readc = (mem.get("Cached", 0) + mem.get("Buffers", 0)
                         + mem.get("SReclaimable", 0))
                others = tot_kib - eng_kib
                pinned = total - eng_kib - others - kernel - readc - free
                snap["pano_engine_gib"] = round(eng_kib / gib, 1)
                snap["pano_procs_gib"] = round(others / gib, 1)
                snap["pano_kernel_gib"] = round(kernel / gib, 1)
                snap["pano_readcache_gib"] = round(readc / gib, 1)
                snap["pano_free_gib"] = round(free / gib, 1)
                snap["pano_pinned_gib"] = round(max(pinned, 0) / gib, 1)
    except Exception as e:
        snap["err_meminfo"] = repr(e)[:100]
    v = to_int(read_text(DRM / "gpu_busy_percent"))
    if v is not None:
        snap["gpu_busy_percent"] = v
    for key, fname in (("gtt_used_gib", "mem_info_gtt_used"),
                       ("vram_used_gib", "mem_info_vram_used"),
                       ("vram_total_gib", "mem_info_vram_total"),
                       ("gtt_total_gib", "mem_info_gtt_total")):
        v = to_int(read_text(DRM / fname))
        if v is not None:
            snap[key] = round(v / (1024 ** 3), 2)
    # 本机为 APU：模型权重走 GTT，"显存"口径 = VRAM+GTT（vram_* 字段保留原义不动）
    vu, gu = snap.get("vram_used_gib"), snap.get("gtt_used_gib")
    if vu is not None or gu is not None:
        snap["gpu_mem_used_gib"] = round((vu or 0) + (gu or 0), 2)
    pw = gpu_power_w()
    if pw is not None:
        snap["gpu_power_w"] = pw
    t = hwmon_temps()
    if "k10temp" in t:
        snap["cpu_temp_c"] = t["k10temp"]
    if "amdgpu" in t:
        snap["gpu_temp_c"] = t["amdgpu"]
    # 引擎重启/计数器回退：清掉缓存的请求速度，禁止显示陈旧速度
    # 双通道：① decode_tokens_total 回退 = 确认重启（永久黑名单，新行才解除）
    # ② metrics 连续 N 拍断供 = 疑似（只清速度、徽标写"指标中断"不谎报重启；
    #    metrics 恢复且无回退 = 误报 → 只解除徽标；疑似黑名单不清，
    #    保留至新日志行（≠指纹）采信 —— 防崩溃循环+暖缓存下死引擎旧行以现速身份复活）
    # _prev_pred 只在拿到数值时更新：1 拍缺口不断链，回退通道可桥接任意间隙
    pred = snap.get("decode_tokens_total")
    restart = False
    confirmed = False
    if isinstance(pred, (int, float)):
        _metrics_miss = 0
        if isinstance(_prev_pred, (int, float)) and pred < _prev_pred:
            restart = True
            confirmed = True
        elif _pending_gap:
            _pending_gap = False
            _speed_stale = None  # 误报只解除徽标；_stale_fp 保留至新行采信（V8-N1）
        _prev_pred = pred
    else:
        _metrics_miss += 1
        if _metrics_miss >= METRICS_MISS_RESTART:
            restart = True
    if restart:
        _last_req.clear()
        if confirmed:
            if _req_fp is not None:
                _confirmed_fp = _req_fp
            _stale_fp = None
        else:
            if _req_fp is not None:
                _stale_fp = _req_fp
        _speed_stale = "restarted" if confirmed else "metrics_gap"
        _pending_gap = not confirmed
    if _speed_stale:
        snap["speed_stale"] = True
        snap["speed_stale_reason"] = _speed_stale
    # w10-ⓕ 总解码吞吐（G2 调研）：decode_tokens_total 墙钟 60s 滑窗差分。
    # 计数器只在请求完成时跳变 → 窗口 ≥60s 抹平阶梯；counter 回退=重启→清窗；
    # processing(in_flight)=0 → 直接 0（不用陈旧窗口尾巴）；metrics 缺口拍不推点（前端 lastGood 保持）。
    # w11-ⓕ2（用户批准，数值准确性）：长生成期间窗口差分≈0（实测 2026-10-29：in_flight=1、
    # busy 71→113s 期间 counter 恒定且 predicted_tokens_seconds=0）→ 主口径改纯实时在途估算：
    # 总吞吐 = in_flight × 最近一次上报的单流速度（引擎仅在请求完成时上报速度，无 per-slot
    # 实时字段：/metrics 无带槽位标签序列、/props /cache 无槽位明细，2026-10-29 侦察）；
    # 尚无速度史（如刚重启）→ 回退 w10 窗口差分；metrics 断供拍两口径都不写键（前端保持+置灰）。
    if restart:
        _decode_win.clear()
        _last_speed = None
    if isinstance(pred, (int, float)) and not restart:
        try:  # w15-bugP0-1：窗口维护段纳入异常保护，⛔ 任何异常冒泡出 collect_once 杀采集节奏（冻结 _latest/_history/_tick）
            _decode_win.append((snap["ts"], pred))
            while _decode_win and snap["ts"] - _decode_win[0][0] > DECODE_WIN_S:
                _decode_win.popleft()
        except Exception:
            pass
    pts = snap.get("predicted_tokens_seconds")
    if isinstance(pts, (int, float)) and pts > 0:
        _last_speed = pts
    ifl = snap.get("in_flight")
    if ifl == 0:
        snap["decode_total_tps"] = 0.0
    elif isinstance(ifl, int) and ifl > 0 and _last_speed and isinstance(pts, (int, float)):
        # w13-ⓐ：主值以本机带宽上限封顶（带宽共享不可加，2-3 槽乘积可超物理上限）
        snap["decode_total_tps"] = round(min(ifl * _last_speed, DECODE_TPS_CAP), 2)
    elif len(_decode_win) >= 2 and snap["ts"] - _decode_win[-1][0] <= 5:
        # 窗口尾点必须新鲜（≤5s）：metrics 断供时不拿陈旧窗口造假鲜值，键缺失→前端 lastGood 保持+置灰
        (tw0, pw0), (tw1, pw1) = _decode_win[0], _decode_win[-1]
        if pw1 >= pw0 and tw1 > tw0:
            # w13-ⓐ：冷启动回退窗口差分同封顶
            snap["decode_total_tps"] = round(min((pw1 - pw0) / (tw1 - tw0), DECODE_TPS_CAP), 2)
    if _last_req:
        snap.update(_last_req)
    # llamacpp:tokens_predicted_total 只在请求完成时跳变，逐周期差分是假速度 —— 不再计算
    n_slots = snap.get("slots")
    in_flight = snap.get("in_flight") or 0
    req_ts = snap.get("req_decode_ts")
    detail = []
    if isinstance(n_slots, int) and n_slots > 0:
        for i in range(min(int(n_slots), 1024)):  # w15-bugP1-4：钳制，⛔ 引擎畸形 slots 打挂采集线程
            busy = i < in_flight
            solo = busy and in_flight == 1
            detail.append({"id": i,
                           "state": "busy" if busy else "idle",
                           "decode_ts": req_ts if solo else None,
                           "req_tokens": snap.get("req_tokens") if solo else None,
                           "req_dur_s": snap.get("req_dur_s") if solo else None})
    snap["slots_detail"] = detail
    with _lock:
        prev = _latest
        for k, d in (("cache_relocated", "ev_relocated"),
                     ("cache_cold_resorts", "ev_cold_resorts")):
            a, b = prev.get(k), snap.get(k)
            ok = isinstance(a, (int, float)) and isinstance(b, (int, float)) and b >= a
            snap[d] = (b - a) if ok else 0
        _latest.clear()
        _latest.update(snap)
        # history 瘦身：只存前端消费的键（死字段/大对象 slots_detail 不进 history）
        _history.append({k: snap[k] for k in HISTORY_KEYS if k in snap})
        _tick += 1
        _cond.notify_all()


def collector():
    while True:
        try:
            collect_once()
        except Exception:
            pass
        time.sleep(INTERVAL)


def slow_collector():
    while True:
        try:
            slow_once()
        except Exception:
            pass
        time.sleep(INTERVAL)


HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>halogen 监控</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cpath d='M16 3l11 6.5v13L16 29 5 22.5v-13L16 3z' fill='none' stroke='%23295ff8' stroke-width='2.4' stroke-linejoin='round'/%3E%3Cpath d='M12.5 12.5v7M19.5 12.5v7M12.5 16h7' fill='none' stroke='%23295ff8' stroke-width='2.4' stroke-linecap='round'/%3E%3C/svg%3E">
<style>
@font-face{font-family:'Space Grotesk';font-style:normal;font-weight:600;font-display:swap;src:url(data:font/woff2;base64,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) format('woff2')}
@font-face{font-family:'Space Grotesk';font-style:normal;font-weight:700;font-display:swap;src:url(data:font/woff2;base64,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) format('woff2')}
:root{
  color-scheme:light;
  --bg:#f4f5f8;--surface:#ffffff;--surface-2:#f6f7f9;
  --fg:#010205;--muted:#6e6f72;--muted-2:#9a9ba0;
  --border:#ebeaed;--border-strong:#d6d7dc;
  --accent:#295ff8;--accent-soft:#eef3fd;
  --chart:#306ffc;
  --up-text:#15803d;--up-bg:#e6faea;
  --down-text:#be185d;--down-bg:#ffe0ec;
  --warn-text:#b45309;--warn-bg:#fef3c7;
  --danger-text:#b91c1c;--danger-bg:#fee9e8;
  --track:#e9ebf0;--bar-off:#8b8f9c;--grid:#e7e7e7;--miss-text:#56585c;
  --hover:rgba(15,23,42,.04);
  --shadow:0 1px 2px rgba(16,24,40,.05),0 1px 3px rgba(16,24,40,.03);
  --r:24px;
  --bar-gap:2px;--bar-r:2px;
  --seg-idle-text:#475569;--seg-idle-bar:#64748b;
  --seg-norm-text:#295ff8;--seg-norm-bar:#4a77f9;
  --seg-high-text:#b45309;--seg-high-bar:#d97706;
  --pano-eng:#4066c4;--pano-proc:#8578bd;--pano-kern:#5d6d88;--pano-read:#5f9fc4;--pano-pin:#57a98c;--pano-free:#b9c4d8;
  --danger-glow:rgba(185,28,28,.4);
  --tip-bg:rgba(15,23,42,.96);--tip-fg:#f8fafc;
  --mono:ui-monospace,"Cascadia Mono",Consolas,"JetBrains Mono",monospace;
  --display:'Space Grotesk',system-ui,-apple-system,"Segoe UI","PingFang SC","Noto Sans CJK SC","Microsoft YaHei",sans-serif;
}
:root[data-theme=dark]{
  color-scheme:dark;
  --bg:#0d1119;--surface:#161d2b;--surface-2:#1c2534;
  --fg:#e9edf5;--muted:#9aa4b8;--muted-2:#7d879c;
  --border:rgba(255,255,255,.10);--border-strong:rgba(255,255,255,.22);
  --accent:#5d9bf5;--accent-soft:rgba(93,155,245,.16);
  --chart:#5d9bf5;
  --up-text:#4ade80;--up-bg:rgba(74,222,128,.15);
  --down-text:#f9739d;--down-bg:rgba(249,115,157,.16);
  --warn-text:#e5b455;--warn-bg:rgba(229,180,85,.15);
  --danger-text:#ff8b86;--danger-bg:rgba(240,101,95,.16);
  --track:rgba(255,255,255,.09);--bar-off:#6b7893;--grid:rgba(255,255,255,.09);--miss-text:#9aa4b8;
  --hover:rgba(255,255,255,.05);
  --shadow:0 1px 2px rgba(0,0,0,.4);
  --seg-idle-text:#94a3b8;--seg-idle-bar:#64748b;
  --seg-norm-text:#5d9bf5;--seg-norm-bar:#5d9bf5;
  --seg-high-text:#e5b455;--seg-high-bar:#f59e0b;
  --pano-eng:#6d9be8;--pano-proc:#a58fd6;--pano-kern:#8494ab;--pano-read:#6fb0d6;--pano-pin:#67b394;--pano-free:#c0ccdf;
  --danger-glow:rgba(255,139,134,.45);
  --tip-bg:rgba(2,6,23,.97);--tip-fg:#e9edf5;
}
*{box-sizing:border-box}
html,body{margin:0}
body{background:var(--bg);color:var(--fg);font-size:14px;line-height:1.5;
  font-family:system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue","PingFang SC","Noto Sans CJK SC","Microsoft YaHei",sans-serif;
  -webkit-font-smoothing:antialiased}
h1,h2,.brand,.k-val,.g-num,.stat .v,.pval{font-family:var(--display)}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px}
button{font:inherit}
.app{display:grid;grid-template-columns:256px minmax(0,1fr);min-height:100vh;max-width:1680px;margin:0 auto}
/* ---------- 侧栏 ---------- */
.sidebar{background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;position:sticky;top:0;height:100vh;overflow-y:auto}
.brand{height:72px;flex:none;display:flex;align-items:center;gap:8px;padding:0 24px;border-bottom:1px solid var(--border);font-size:18px;font-weight:700}
.brand svg{width:28px;height:28px;flex:none}
.side-sec{padding:24px;border-bottom:1px solid var(--border);min-width:0}
.side-title{font-size:12px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin:0 0 8px;display:flex;align-items:center;gap:8px}
.model-name{font-size:14px;font-weight:600;font-family:var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:8px}
.kv-row{display:flex;justify-content:space-between;align-items:baseline;gap:8px;font-size:13px;color:var(--muted);padding:4px 0}
.kv-row b{font-weight:600;color:var(--fg);font-variant-numeric:tabular-nums;font-family:var(--display);text-align:right;overflow-wrap:anywhere}
.slot-li{display:flex;align-items:center;gap:8px;padding:4px 8px;margin:0 -8px;border-radius:12px;font-size:13px;font-variant-numeric:tabular-nums}
.slot-li:hover{background:var(--hover)}
.slot-li .spd{margin-left:auto;font-weight:600;font-family:var(--display)}
.slot-li .idle-t{margin-left:auto;color:var(--muted);font-weight:400}
.slot-li .slot-n{font-family:var(--mono);font-weight:600}  /* w13-ⓒ→w17-②：槽位编号数字=--mono 等宽栈（与采集健康状态列 code 同款，用户多次指正 ⛔ 勿回退 display/Space Grotesk）；行本身中文保持正文族（w10-ⓒ） */
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;flex:none}
.on{background:var(--up-text);animation:breathe 2.6s ease-in-out infinite}
.off{background:var(--danger-text);animation:breathe 1.6s ease-in-out infinite}
.wait{background:var(--bar-off)}
.sleep{background:var(--accent);animation:breathe 2.2s ease-in-out infinite}
.busy{background:var(--up-text);animation:breathe 1.8s ease-in-out infinite}
.idle{background:var(--bar-off)}
@keyframes breathe{0%,100%{opacity:1}50%{opacity:.4}}
.dot:hover,.breath:hover,.st-pill:hover,.skel:hover{animation-play-state:paused}
.skel:hover::after{animation-play-state:paused}
.side-foot{margin-top:auto;padding:24px;display:flex;flex-direction:column;gap:8px}
.qsep{border-top:1px solid var(--border);margin:16px 0 24px}
/* w11-ⓞ 侧栏第三模块对齐：tabular-nums、基线对齐；「次/회」单位绝对定位悬挂，⛔ 不占对齐线
   w12-ⓐ ? 按钮移至模块标题右侧（与其他模块同款 side-title）
   w17-①：废弃 2×2 subgrid + 竖分隔线（竖线两侧净空左窄右宽，用户判不对称，放弃摆布）——
   改 4 行（处理中/排队请求/排队等待/已放弃 各一行），行结构=.kv-row 同款：
   flex + space-between 两端对齐（标签左、值右），13px/行高1.5/padding 4px 0，
   行间距实测 27.5px 与「模型信息」「采集健康」逐像素一致 */
.qblock{position:relative;padding:4px 8px;margin:0 -8px;border-radius:12px;transition:background-color .3s}
.qblock.alert{background:var(--danger-bg);animation:qbreath 2s ease-in-out infinite}
.qblock.alert #q-queued{font-size:16px;font-weight:700;color:var(--danger-text)}
.qrow{display:flex;justify-content:space-between;align-items:baseline;gap:8px;padding:4px 0;font-size:13px;color:var(--muted);font-variant-numeric:tabular-nums}
.qrow b{color:var(--fg);font-weight:600;font-family:var(--display);text-align:right}
.qnw{position:relative;text-align:right}
.qu{position:absolute;left:calc(100% + 1px);top:0;color:var(--muted);font-weight:400}
.qrow>span{overflow-wrap:break-word;hyphens:auto;min-width:0}
@keyframes qbreath{0%,100%{box-shadow:0 0 0 0 var(--danger-glow)}50%{box-shadow:0 0 0 7px transparent}}
.h1w{display:flex;align-items:center;gap:8px;min-width:0}
.h1-alert{width:10px;height:10px;border-radius:50%;background:var(--danger-text);flex:none;animation:qbreath 2s ease-in-out infinite}
/* w10-ⓖ 速率类防归零：断供保持上一有效值+置灰 */
.stale{color:var(--muted)!important;opacity:.6}
.hrow{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted)}
.hrow+.hrow{margin-top:8px}
.hrow code{font-family:var(--mono);font-size:12px}
.hst{margin-left:auto;font-weight:400;color:var(--muted)}
.hst.bad{color:var(--danger-text)}
.dot.h-ok{background:var(--up-text)}
.dot.h-bad{background:var(--danger-text)}
.side-btn{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;font-size:13px;font-weight:600;color:var(--muted);background:transparent;border:1px solid var(--border);border-radius:999px;padding:8px 16px;cursor:pointer;white-space:nowrap;transition:color .15s,border-color .15s,background-color .15s}
.side-btn:hover{color:var(--fg);border-color:var(--border-strong);background:var(--hover)}
.side-btn svg{width:15px;height:15px;fill:currentColor;flex:none}
/* ---------- 主区 ---------- */
main{min-width:0;display:flex;flex-direction:column}
.topbar{height:72px;flex:none;display:flex;align-items:center;justify-content:space-between;gap:16px;padding:0 32px;border-bottom:1px solid var(--border);background:var(--surface);position:sticky;top:0;z-index:10}
h1{font-size:20px;font-weight:600;margin:0;letter-spacing:-.01em}
.top-right{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted);font-variant-numeric:tabular-nums;min-width:0;flex-wrap:wrap;justify-content:flex-end}
.status-pill{display:inline-flex;align-items:center;gap:8px;padding:8px 16px;border-radius:999px;background:var(--surface-2);border:1px solid var(--border);font-size:13px;font-weight:600;color:var(--fg);white-space:nowrap}
.time-chip{display:inline-flex;align-items:center;gap:8px;padding:8px 16px;border-radius:999px;background:var(--surface-2);border:1px solid var(--border);color:var(--muted);white-space:nowrap}
.time-chip svg{width:14px;height:14px;flex:none;stroke:currentColor;fill:none;stroke-width:1.4}
.top-btn{width:auto}
.lang-wrap{position:relative;display:inline-flex}
.side-btn .lang-caret{width:12px;height:12px;fill:none;stroke:currentColor;stroke-width:1.8;transition:transform 120ms ease}
#langBtn[aria-expanded=true] .lang-caret{transform:rotate(180deg)}
.lang-menu{position:absolute;top:calc(100% + 6px);right:0;min-width:148px;display:none;flex-direction:column;gap:2px;padding:6px;background:var(--surface);border:1px solid var(--border-strong);border-radius:12px;box-shadow:var(--shadow),0 12px 32px rgba(2,6,23,.18);z-index:200}
:root[data-theme=dark] .lang-menu{background:var(--surface-2);box-shadow:var(--shadow),0 12px 32px rgba(0,0,0,.6)}
.lang-menu.open{display:flex}
.lang-opt{display:flex;align-items:center;gap:8px;padding:8px 12px;border-radius:8px;font-size:13px;font-weight:600;color:var(--fg);cursor:pointer;white-space:nowrap}
.lang-opt:hover,.lang-opt:focus-visible{background:var(--hover)}
.lang-opt[aria-selected=true]{color:var(--accent)}
.content{padding:32px;display:flex;flex-direction:column;gap:24px;min-width:0;flex:1 1 auto}
.card{background:var(--surface);border-radius:var(--r);box-shadow:var(--shadow);padding:24px;min-width:0;transition:box-shadow .15s}
.card:hover{box-shadow:0 2px 6px rgba(16,24,40,.08),0 1px 3px rgba(16,24,40,.04)}
.card h2{font-size:18px;font-weight:600;margin:0 0 16px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.card.alert{outline:1.5px solid var(--danger-text)}
/* KPI */
.kpis{display:grid;grid-template-columns:1fr;gap:24px;flex:none}
@media(min-width:560px){.kpis{grid-template-columns:repeat(2,1fr)}}
@media(min-width:1200px){.kpis{grid-template-columns:repeat(4,1fr)}}
.kpi{display:flex;flex-direction:column;gap:8px;padding:24px}
.k-lbl{font-size:16px;font-weight:600;display:flex;align-items:center;justify-content:space-between;gap:8px}
.k-lbl>span{display:inline-flex;align-items:center;gap:8px;min-width:0}
.k-lbl svg{width:18px;height:18px;flex:none;stroke:var(--accent);fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.k-val{font-size:30px;font-weight:700;letter-spacing:-.02em;line-height:1.2;font-variant-numeric:tabular-nums;display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
.k-num{display:inline-flex;align-items:baseline;gap:8px;white-space:nowrap}
.k-val .unit{font-size:14px;font-weight:500;color:var(--muted)}
/* w10-ⓕ 解码速度副值行；w11-ⓝ 只剩峰值：flex 布局数字/单位 flex:none 永不截断（标签可收缩），
   并用行距回收抵消行高（gap 8→3、副行/主值 line-height→1），卡高回归 w9 原高 */
.k-sub{font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums;display:flex;align-items:baseline;gap:4px;line-height:1}
.k-sub b{color:var(--fg);font-weight:600;flex:none}
.k-sub .ks-u{flex:none}
.k-sub .ks-l{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#kc-spd{gap:3px}
#kc-spd .k-val{line-height:1}
.kpi.seg-idle .k-num{color:var(--seg-idle-text)}
.kpi.seg-norm .k-num{color:var(--seg-norm-text)}
.kpi.seg-high .k-num{color:var(--seg-high-text)}
/* w12-ⓓ：迷你柱收细 1/3（高 36→24、柱宽 flex:1→9px 固定+两端分布），颜色/数量/语义不动 */
.mibars{display:flex;align-items:flex-end;justify-content:space-between;gap:var(--bar-gap);height:24px;margin-top:auto;position:relative}
.mibars i{flex:none;width:9px;min-width:0;border-radius:var(--bar-r);background:var(--track);height:2px}
.kpi.seg-idle .mibars i{background:var(--seg-idle-bar)}
.kpi.seg-norm .mibars i{background:var(--seg-norm-bar)}
.kpi.seg-high .mibars i{background:var(--seg-high-bar)}
.mibars.wait i{background:var(--track)}
.mibars.wait::after{content:'';position:absolute;inset:0;transform:translateX(-100%);background:linear-gradient(90deg,transparent,var(--accent-soft),transparent);animation:shimmer 1.8s infinite}
.badge{display:inline-flex;align-items:center;font-size:12px;font-weight:600;padding:4px 8px;border-radius:999px;font-variant-numeric:tabular-nums;white-space:nowrap}
.badge.up{color:var(--up-text);background:var(--up-bg)}
.badge.down{color:var(--down-text);background:var(--down-bg)}
.badge.flat{color:var(--miss-text);background:var(--track)}
.badge.warn{color:var(--warn-text);background:var(--warn-bg)}
.badge.bad{color:var(--danger-text);background:var(--danger-bg)}
.badge.miss{color:var(--miss-text);background:var(--track)}
/* bento 两行 */
.bento{display:grid;grid-template-columns:minmax(0,62fr) minmax(0,38fr);gap:24px;align-items:stretch;flex:1 1 auto;min-height:340px}
.bento.bot{flex:1 1 auto;min-height:260px}
@media(max-width:1119px){.bento,.bento.bot{grid-template-columns:minmax(0,1fr);flex:0 0 auto;min-height:0}}
#c-kv,#c-pool,#c-pc,#c-pano{display:flex;flex-direction:column;min-width:0}
/* 扇叶仪表（v17 放射刻度扇，恢复） */
.gauge{display:flex;justify-content:center;flex:none;padding:4px 0 0}
.gwrap{position:relative;width:min(178px,100%)}
.gwrap svg{width:100%;height:auto;display:block}
.tk{stroke:var(--track);stroke-width:5.5;stroke-linecap:round;transition:stroke .3s}
.gwrap.seg-idle .tk.on{stroke:var(--seg-idle-bar)}
.gwrap.seg-norm .tk.on{stroke:var(--seg-norm-bar)}
.gwrap.seg-high .tk.on{stroke:var(--seg-high-bar)}
/* w12-ⓑ：--g-ink-dy = 数字字形墨迹盒补偿（JS 按 canvas actualBoundingBox 实测注入）；--g-anchor 读图微调 */
.g-center{position:absolute;left:0;right:0;top:var(--g-anchor,63%);transform:translateY(calc(-50% + var(--g-ink-dy,0px)));text-align:center;pointer-events:none}
.g-num{font-size:30px;font-weight:700;font-variant-numeric:tabular-nums;letter-spacing:-.02em}
.g-num .unit{font-size:14px;font-weight:500;color:var(--muted)}
/* w12-ⓒ：拆两行（第一行 已用/总量 与扇形同口径；第二行独立浅色小字 预留，tooltip 预留口径）
   w16-③：预留行移出 .kvl、.kv-used 改 flex 列居中——两行各自独立水平居中于仪表轴，
   ⛔ 再随第一行宽度变化而偏位（有值/无值/0 值三态恒居中） */
.kv-used{display:flex;flex-direction:column;align-items:center;text-align:center;font-size:13px;color:var(--muted);font-variant-numeric:tabular-nums;margin-top:8px;flex:none;font-family:var(--display)}
.kv-used .kvl{text-align:left}
.kv-used b{color:var(--fg);font-weight:600}
.kv-l2{font-size:12px;color:var(--muted-2);margin-top:2px;text-align:center}  /* w13-ⓕ：预留行居中（与第一行同轴） */
.kv-l2 b{color:var(--muted-2)}
/* stat 块 */
.stat-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:8px;margin-top:16px;flex:none}
.stat{background:var(--surface-2);border-radius:16px;padding:16px;min-width:0;position:relative;overflow:hidden}
.stat .v{font-size:17px;font-weight:600;line-height:1.3;font-variant-numeric:tabular-nums;letter-spacing:-.01em;overflow-wrap:anywhere;text-align:center}
/* w9-F：占用率卡 3 小卡等高、标题水平对齐（badge 比纯数字高 2.5px 会把标题挤下，统一 .v 高度并垂直居中）
   w16-①：推广到全部 stat 小卡（含 Prompt 缓存）——骨架态与有值态 .v 盒高恒等 26px，标签不随状态跳动 */
.stat .v{min-height:26px;display:flex;align-items:center;justify-content:center}
.stat .lbl{font-size:12px;color:var(--muted);margin-top:8px;text-align:center}
/* 池压力 2 列 */
.pgrid{display:grid;grid-template-columns:1fr 1fr;grid-auto-rows:1fr;gap:8px 24px;flex:1 1 auto}
/* w12-ⓕ：标题贴顶、数字 margin:auto 在剩余空间垂直居中 → 数字上下留白恒等（自适应行高） */
.pitem{display:flex;flex-direction:column;padding:8px 0;border-top:1px solid var(--border);min-width:0}
.pname{font-size:13px;color:var(--muted)}
.pval{font-size:17px;font-weight:600;font-variant-numeric:tabular-nums;text-align:center;white-space:nowrap;margin:auto 0}
/* 内存全景（w9-E：与 KV 占用率仪表同款放射刻度环，外径同 .gwrap=178 基准；中心=总量定义；V18a①：吸收卡内余量） */
.donut{position:relative;flex:1 1 auto;min-height:118px;display:flex;align-items:center;justify-content:center}
.donut .prwrap{position:relative;width:min(178px,100%)}
.prwrap svg{width:100%;height:auto;display:block}
#pn-ticks .pk-eng{stroke:var(--pano-eng)}
#pn-ticks .pk-proc{stroke:var(--pano-proc)}
#pn-ticks .pk-kern{stroke:var(--pano-kern)}
#pn-ticks .pk-read{stroke:var(--pano-read)}
#pn-ticks .pk-free{stroke:var(--pano-free)}
#pn-ticks .pk-pin{stroke:var(--pano-pin)}
.donut-center{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;pointer-events:none;text-align:center}
.dn-num{font-family:var(--display);font-size:17px;font-weight:700;font-variant-numeric:tabular-nums;letter-spacing:-.02em;line-height:1.2;white-space:nowrap}
.dn-sub{position:absolute;left:0;right:0;top:calc(50% + 11px);font-size:11px;color:var(--muted);font-variant-numeric:tabular-nums}
#c-pano .legend{margin-top:12px;flex:none}
.legend{display:flex;gap:8px 16px;font-size:12px;color:var(--muted);flex-wrap:wrap;font-variant-numeric:tabular-nums;justify-content:center;font-family:var(--display)}
.legend i{display:inline-block;width:8px;height:8px;border-radius:50%;flex:none}
.lg-item{display:inline-flex;align-items:center;gap:8px;min-width:0}
.lg-item b{color:var(--fg);font-weight:600}
/* spark（图内两端时间标注，图外零小字；N1：min-height:0 防 flex 固有高溢出） */
.sparkgrp{flex:1 1 0;min-height:48px;display:flex;flex-direction:column;padding-top:12px}
.sparkbox{position:relative;min-width:0;min-height:0;flex:1 1 auto}
svg.spark{width:100%;height:100%;min-height:0;display:block}
.stime{position:absolute;bottom:0;font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums;pointer-events:none;font-family:var(--display)}
.stime.l{left:2px}
.stime.r{right:2px}
.sp-empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;gap:8px;font-size:12px;color:var(--muted)}
.sparktip{position:absolute;top:0;transform:translate(-50%,-115%);display:none;background:var(--fg);color:var(--surface);border-radius:8px;padding:8px;font-size:12px;font-weight:600;font-variant-numeric:tabular-nums;white-space:nowrap;pointer-events:none;z-index:5;font-family:var(--display)}
/* 空态骨架 */
/* w16-①：骨架条与所在位真实文本同字号同行高——高度=0.72em（≈cap-height 墨迹带），
   vertical-align:baseline 使灰条底边落在文字基线上（与真实文本同基线）；
   宽度按各显示位真实值宽收窄（⛔ 56px 通用宽挤爆窄值列） */
.skel{display:inline-block;height:0.72em;width:88px;border-radius:7px;background:var(--track);position:relative;overflow:hidden;vertical-align:baseline}
.skel.lg{height:38px;width:170px;border-radius:10px}
.skel.sm{height:0.72em;width:56px}
.skel.sk-kpi{height:0.72em;width:72px;border-radius:8px;align-self:center;flex:none}
.skel.gsk{height:0.72em;width:64px;border-radius:8px}
.qrow b.skel{width:24px}
.lg-item b.skel{width:26px}
.kv-l2 b.skel{width:22px}
#donut-total.skel{width:40px}
.stat .v.skel{width:56px;margin:0 auto}  /* w16-①：小卡骨架=56×26 盒水平居中（与有值态居中文本同位） */
.skel::after{content:'';position:absolute;inset:0;transform:translateX(-100%);background:linear-gradient(90deg,transparent,var(--accent-soft),transparent);animation:shimmer 1.8s infinite}
@keyframes shimmer{100%{transform:translateX(100%)}}
.breath{width:9px;height:9px;border-radius:50%;background:var(--accent);animation:breathe 2.2s ease-in-out infinite;flex:none}
/* 自绘 tooltip */
.tip{position:fixed;display:none;background:var(--tip-bg);color:var(--tip-fg);font-size:12px;line-height:1.6;font-weight:500;padding:8px 12px;border-radius:8px;max-width:300px;white-space:pre-line;pointer-events:none;z-index:99;box-shadow:0 4px 16px rgba(0,0,0,.25);font-family:var(--display)}
.qh{display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;flex:none;border-radius:50%;border:1px solid var(--border-strong);background:var(--surface-2);color:var(--muted);font-size:11px;font-weight:700;line-height:1;cursor:help;transition:color .15s,border-color .15s,background-color .15s}
.qh:hover,.qh:focus-visible{color:var(--accent);border-color:var(--accent);background:var(--accent-soft)}
/* 其他 */
.red{color:var(--danger-text)}.yellow{color:var(--warn-text)}
.foot{color:var(--muted);font-size:12px;font-variant-numeric:tabular-nums;flex:none;font-family:var(--display);display:flex;align-items:center;gap:6px}
.foot svg{width:13px;height:13px;flex:none;stroke:var(--accent);fill:none;stroke-width:1.6;stroke-linecap:round;stroke-linejoin:round}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
.paused *{animation-play-state:paused!important}
@media(max-width:1559px){.topbar{height:auto;min-height:72px}}
/* 窄屏：侧栏转顶部面板 */
@media(max-width:1023px){
  .app{grid-template-columns:minmax(0,1fr)}
  .sidebar{width:auto;position:static;height:auto;border-right:0;border-bottom:1px solid var(--border);flex-direction:row;flex-wrap:wrap;align-items:stretch}
  .brand{border-bottom:0}
  .side-sec{border-bottom:0;border-left:1px solid var(--border);flex:1 1 220px}
  .side-foot{margin:0 0 0 auto;border-top:0;flex-direction:column;justify-content:center;padding:16px 24px}
  .content{padding:24px 16px}
  .topbar{padding:0 16px}
}
@media(max-width:559px){
  .side-sec{border-left:0;border-top:1px solid var(--border);flex:1 1 100%}
  .side-foot{margin:0;flex-direction:row;align-items:center}
  .topbar{height:auto;flex-wrap:wrap;row-gap:8px;padding:12px 16px}
  h1{font-size:20px}
  .btn-txt{display:none}
}
</style>
</head>
<body>
<div class="tip" id="tip" role="tooltip"></div>
<div class="app">
<aside class="sidebar">
  <div class="brand"><svg viewBox="0 0 32 32" aria-hidden="true"><path d="M16 3l11 6.5v13L16 29 5 22.5v-13L16 3z" fill="none" stroke="var(--accent)" stroke-width="2.4" stroke-linejoin="round"/><path d="M12.5 12.5v7M19.5 12.5v7M12.5 16h7" fill="none" stroke="var(--accent)" stroke-width="2.4" stroke-linecap="round"/></svg><span data-i18n="brandSide">halogen 监控</span></div>
  <div class="side-sec">
    <div class="side-title" data-i18n="modelInfo">模型信息</div>
    <div class="model-name" id="m-name">—</div>
    <div class="kv-row"><span data-i18n="ctxLen">上下文长度</span><b id="m-ctx">—</b></div>
    <div class="kv-row"><span data-i18n="kvPrec">KV 缓存精度</span><b id="m-kv">—</b></div>
    <div class="kv-row"><span data-i18n="weightFmt">权重格式</span><b id="m-fmt">—</b></div>
    <div class="kv-row"><span data-i18n="engineVer">引擎版本</span><b id="m-ver">—</b></div>
    <div class="kv-row"><span data-i18n="slotCount">槽位数</span><b id="m-slots">—</b></div>
  </div>
  <div class="side-sec">
    <div class="side-title"><span data-i18n="slotState">槽位状态</span> <button class="qh" type="button" tabindex="0" aria-label="槽位状态说明" data-i18n-aria="slotStateAria" data-tip="引擎仅在推理完成时上报速度，不提供单槽位数据。展示从 1 起，后端编号从 0 起。" data-i18n-tip="slotStateTip">?</button></div>
    <div id="side-slots"></div>
    <div class="qsep"></div>
    <div class="side-title"><span data-i18n="reqStatus">请求状态</span> <button class="qh" type="button" tabindex="0" aria-label="处理中说明" data-i18n-aria="inFlightAria" data-tip="处理中 = 引擎正在处理的请求数；排队请求 = 等待槽位的请求数。排队等待 = 等待≥1 秒的次数；已放弃 = 客户端断连或排队超时。" data-i18n-tip="inFlightTip">?</button></div>
    <div class="qblock" id="q-block">
    <div class="qrow" id="q-flight-row"><span data-i18n="inFlight">处理中</span><b id="q-flight">—</b></div>
    <div class="qrow" id="q-queued-row"><span data-i18n="queuedReq">排队请求</span><b id="q-queued">—</b></div>
    <div class="qrow" id="q-wait-row"><span data-i18n="queueWaits">排队等待</span><span class="qnw"><b id="q-waited">—</b><span class="qu" data-i18n="timesUnit">次</span></span></div>
    <div class="qrow" id="q-gave-row"><span data-i18n="abandoned">已放弃</span><b id="q-gave">—</b></div>
    </div>
  </div>
  <div class="side-sec side-health">
    <div class="side-title"><span data-i18n="colHealth">采集健康</span> <button class="qh" type="button" tabindex="0" aria-label="采集健康说明" data-i18n-aria="colHealthAria" data-tip="四条采集通道（health/metrics/cache/props）本轮取数状态：绿点正常，红点异常（判异常前已自动重试）。通道异常期间，各卡片保留上一次有效数值不空白，恢复后自动更新。" data-i18n-tip="colHealthTip">?</button></div>
    <div class="hrow" data-tip="数据用于：顶栏引擎状态、槽位状态、KV 缓存池占用率&#10;（槽位数 / 处理中 / 排队请求 / 总量）" data-i18n-tip="tipHealth"><span class="dot wait" id="h-health"></span><span data-i18n="probeLiveness">活性探测</span><code>health</code><span class="hst" id="hs-health">采集中…</span></div>
    <div class="hrow" data-tip="数据用于：KPI 四卡、KV 缓存池占用率与走势、投机接受率、侧栏处理中/排队请求/排队等待/已放弃计数" data-i18n-tip="tipMetrics"><span class="dot wait" id="h-metrics"></span><span data-i18n="probeMetrics">指标采集</span><code>metrics</code><span class="hst" id="hs-metrics">采集中…</span></div>
    <div class="hrow" data-tip="数据用于：KV 缓存池压力、Prompt 缓存" data-i18n-tip="tipCache"><span class="dot wait" id="h-cache"></span><span data-i18n="probeCache">缓存状态</span><code>cache</code><span class="hst" id="hs-cache">采集中…</span></div>
    <div class="hrow" data-tip="数据用于：侧栏模型信息（别名/槽位数）、待命中状态判定" data-i18n-tip="tipProps"><span class="dot wait" id="h-props"></span><span data-i18n="probeProps">引擎属性</span><code>props</code><span class="hst" id="hs-props">采集中…</span></div>
  </div>
  <div class="side-foot">
    <button class="side-btn" id="themeBtn" type="button" aria-label="切换亮色/暗色主题" data-i18n-aria="themeAria">
      <svg id="ic-moon" viewBox="0 0 16 16" aria-hidden="true"><path d="M13.5 9.6A6 6 0 0 1 6.4 2.5a6 6 0 1 0 7.1 7.1z"/></svg>
      <svg id="ic-sun" viewBox="0 0 16 16" aria-hidden="true" style="display:none"><circle cx="8" cy="8" r="3.2"/><g stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><path d="M8 .8v1.8M8 13.4v1.8M.8 8h1.8M13.4 8h1.8M2.9 2.9l1.3 1.3M11.8 11.8l1.3 1.3M13.1 2.9l-1.3 1.3M4.2 11.8l-1.3 1.3"/></g></svg>
      <span class="btn-txt" id="themeTxt">暗色</span>
    </button>
  </div>
</aside>
<main>
  <header class="topbar">
    <div class="h1w"><h1 data-i18n="overviewH1">推理监控总览</h1><span class="h1-alert" id="h1-alert" style="display:none" aria-hidden="true"></span></div>
    <div class="top-right">
      <span class="status-pill" id="status-pill"><span class="dot wait" id="dot"></span><span id="status-txt">连接中…</span></span>
      <span class="time-chip"><svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6.4"/><path d="M8 4.6V8l2.4 1.6" stroke-linecap="round"/></svg><span id="sub" aria-live="polite">加载中…</span></span>
      <div class="lang-wrap" id="langWrap">
        <button class="side-btn top-btn" id="langBtn" type="button" aria-label="切换语言" data-i18n-aria="langSwitchAria" aria-haspopup="listbox" aria-expanded="false" aria-controls="langMenu">
          <svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6.4" fill="none" stroke="currentColor" stroke-width="1.3"/><path d="M1.6 8h12.8M8 1.6c2.1 2.3 2.1 10.5 0 12.8-2.1-2.3-2.1-10.5 0-12.8z" fill="none" stroke="currentColor" stroke-width="1.3"/></svg>
          <span class="btn-txt" id="langTxt">中文</span>
          <svg class="lang-caret" viewBox="0 0 16 16" aria-hidden="true"><path d="M4 6.5 8 10.5 12 6.5" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </button>
        <div class="lang-menu" id="langMenu" role="listbox" aria-label="切换语言" data-i18n-aria="langSwitchAria"></div>
      </div>
      <button class="side-btn top-btn" id="pauseBtn" type="button" aria-pressed="false" aria-label="暂停刷新">
        <svg id="ic-pause" viewBox="0 0 16 16" aria-hidden="true"><rect x="3" y="2" width="4" height="12" rx="1"/><rect x="9" y="2" width="4" height="12" rx="1"/></svg>
        <svg id="ic-play" viewBox="0 0 16 16" aria-hidden="true" style="display:none"><path d="M4.5 2.2v11.6a.8.8 0 0 0 1.22.68l9.1-5.8a.8.8 0 0 0 0-1.36l-9.1-5.8A.8.8 0 0 0 4.5 2.2z"/></svg>
        <span class="btn-txt" id="pauseTxt">暂停刷新</span>
      </button>
    </div>
  </header>
  <div class="content">

    <section class="kpis" aria-label="核心指标" data-i18n-aria="coreMetrics">
      <div class="card kpi" id="kc-spd"><div class="k-lbl"><span><span data-i18n="decodeSpeed">解码速度</span> <button class="qh" type="button" tabindex="0" aria-label="解码速度说明" data-i18n-aria="decodeSpeedAria" data-tip="主值 = 总解码速度（实时估算）：在途请求数 × 最近一次上报的单流速度。引擎仅在请求完成时上报速度（生成进行中上报为 0），故在途产出按最近上报的单流速度估算；空闲显示 0。显示以本机带宽上限封顶。&#10;峰值 = 最近 30 分钟内总解码速度的最高值。" data-i18n-tip="decodeSpeedTip">?</button></span><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 18a9 9 0 1 1 16 0"/><path d="M12 14l3.8-4.6"/><circle cx="12" cy="15" r="1.4"/></svg></div>
        <div class="k-val"><span class="k-num"><span id="k-spd-v" class="skel sk-kpi"></span><span class="unit" id="k-spd-u" style="display:none">tok/s</span></span></div>
        <div class="k-sub"><span class="ks-l" data-i18n="peak30">30m 峰值</span> <b id="k-spd-p">—</b> <span class="ks-u">tok/s</span></div>
        <div class="mibars wait" id="mb-spd"></div></div>
      <div class="card kpi" id="kc-gpu"><div class="k-lbl"><span data-i18n="gpuUtil">GPU 利用率</span><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="5" width="14" height="14" rx="2.5"/><path d="M9 9.5h6M9 13h4"/><path d="M9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3"/></svg></div>
        <div class="k-val"><span class="k-num"><span id="k-gpu-v" class="skel sk-kpi"></span><span class="unit" id="k-gpu-u" style="display:none">%</span></span></div>
        <div class="mibars wait" id="mb-gpu"></div></div>
      <div class="card kpi" id="kc-temp"><div class="k-lbl"><span data-i18n="gpuTemp">GPU 温度</span><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M10 13.5V5a2 2 0 1 1 4 0v8.5a4.5 4.5 0 1 1-4 0z"/><path d="M12 9v5"/></svg></div>
        <div class="k-val"><span class="k-num"><span id="k-temp-v" class="skel sk-kpi"></span><span class="unit" id="k-temp-u" style="display:none">°C</span></span></div>
        <div class="mibars wait" id="mb-temp"></div></div>
      <div class="card kpi" id="kc-pow"><div class="k-lbl"><span><span data-i18n="gpuPower">GPU 功耗</span> <button class="qh" type="button" tabindex="0" aria-label="GPU 功耗说明" data-i18n-aria="gpuPowerAria" data-tip="均衡模式 85 W 为 LLM 推理的最佳功耗区间，120 W 性能模式对推理性能的提升仅约 2%。" data-i18n-tip="gpuPowerTip">?</button></span><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M13 2 4.5 13.5H11L10 22l8.5-11.5H12L13 2z"/></svg></div>
        <div class="k-val"><span class="k-num"><span id="k-pow-v" class="skel sk-kpi"></span><span class="unit" id="k-pow-u" style="display:none">W</span></span></div>
        <div class="mibars wait" id="mb-pow"></div></div>
    </section>

    <section class="bento">
      <div class="card" id="c-kv">
        <h2><span data-i18n="kvUsage">KV 缓存池占用率</span>
          <button class="qh" type="button" tabindex="0" aria-label="KV 缓存池占用率说明" data-i18n-aria="kvUsageAria" data-tip="占用率 = KV 缓存池已用位置占比。&#10;占用率 100% 不代表过载：池满时排队为 0、冷重算为 0 是常态，引擎会按设计将内存分配至饱和。&#10;占用率下降有两种途径：① 版本替换回收（旧版本缓存被新版本替换，代价为零）② LRU 淘汰（被移出池的会话在下次请求时重新执行一次 prefill，冷重算 +1）&#10;预留 = 在途请求声明的空间需求（prompt + max_tokens，含尚未生成的部分），与已用口径独立、不计入已用" data-i18n-tip="kvUsageTip">?</button>
          <span class="badge bad" id="kv-badge" style="display:none" data-i18n="queuing">缓存等待</span>
          <span class="badge warn" id="kv-sleep" style="display:none" data-i18n="modelSleeping">模型待命中</span>
          <span class="badge miss" id="kv-miss" style="display:none" data-i18n="missMetrics">metrics 数据缺失</span></h2>
        <div class="gauge">
          <div class="gwrap" id="g-wrap">
            <svg viewBox="0 0 200 152" aria-hidden="true"><g id="g-ticks"></g></svg>
            <div class="g-center"><span class="g-num"><span id="g-num" class="skel gsk"></span><span class="unit" id="g-unit" style="display:none">%</span></span></div>
          </div>
        </div>
        <div class="kv-used"><div class="kvl"><div><span data-i18n="used">已用</span> <b id="kv-used-t">—</b> / <span data-i18n="total">总量</span> <b id="kv-used-p">—</b></div></div><div class="kv-l2" data-tip="预留 = 在途请求声明的空间需求（prompt + max_tokens，含尚未生成的部分），与已用口径独立、不计入已用" data-i18n-tip="reservedTip"><span data-i18n="reserved">预留</span> <b id="kv-used-r">—</b></div></div>
        <div class="sparkgrp">
          <div class="sparkbox" id="sb-kv">
            <svg class="spark" viewBox="0 0 480 72" preserveAspectRatio="none" aria-hidden="true"><defs><linearGradient id="gsp-kv" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="var(--chart)" stop-opacity=".25"/><stop offset="1" stop-color="var(--chart)" stop-opacity="0"/></linearGradient></defs><path class="sa" fill="url(#gsp-kv)" stroke="none"/><path class="sl" fill="none" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/><circle class="se" r="0"/><line class="cr" y1="0" y2="72" style="display:none" stroke="var(--border-strong)" stroke-width="1" vector-effect="non-scaling-stroke"/></svg>
            <span class="stime l" id="st-kv0"></span><span class="stime r" id="st-kv1"></span>
            <div class="sp-empty" id="sbe-kv" style="display:none"><span class="breath"></span><span id="sbe-kv-t" data-i18n="waitingData">等待数据…</span></div>
            <div class="sparktip"></div>
          </div>
        </div>
        <div class="stat-row">
          <div class="stat o"><div class="v" id="kv-draft"><span class="skel sm"></span></div><div class="lbl" data-i18n="specAccept">投机接受率</div></div>
          <div class="stat"><div class="v" id="kv-req"><span class="skel sm"></span></div><div class="lbl" data-i18n="totalReqs">累计请求</div></div>
          <div class="stat o"><div class="v"><span class="badge flat" id="kv-queue" data-i18n="collecting">采集中…</span></div><div class="lbl" data-i18n="queueState">排队状态</div></div>
        </div>
      </div>
      <div class="card" id="c-pool">
        <h2><span data-i18n="poolPressure">KV 缓存池压力</span>
          <button class="qh" type="button" tabindex="0" aria-label="KV 缓存池压力说明" data-i18n-aria="poolPressureAria" data-tip="缓存等待：正在等待 KV 缓存池腾出空间的请求数，大于 0 需立即关注；占用率不高时也可能出现：池内空间碎片化或等待槽位释放&#10;累计等待：请求为等待空间累计耗费的时间（秒），只增不减&#10;冷重算次数：旧会话被移出 KV 缓存池后重新执行 prefill 的次数，持续增长说明池容量趋于不足&#10;占位会话数：已离开但仍保留 KV 缓存占用的会话数，可随时复用&#10;快照搬移次数：缓存快照被搬移的累计次数，与冷重算次数结合观察&#10;碎片整理次数：KV 缓存池碎片整理（紧凑化）的累计次数，增长较快说明碎片较多&#10;压缩预留次数：新请求预留空间被压缩的累计次数，增长说明池内空间趋于紧张&#10;快照搬移/碎片整理/压缩预留均属 KV 缓存池空间回收机制，日常增长属正常调度行为" data-i18n-tip="poolPressureTip">?</button>
          <span class="badge bad" id="pool-badge" style="display:none" data-i18n="queuing">缓存等待</span><span class="badge miss" id="pool-miss" style="display:none" data-i18n="missCache">cache 数据缺失</span></h2>
        <div class="pgrid">
          <div class="pitem"><div class="pname" data-i18n="queuing">缓存等待</div><div class="pval" id="pv-waiting">—</div></div>
          <div class="pitem"><div class="pname" data-i18n="cumWait">累计等待</div><div class="pval" id="pv-waitings">—</div></div>
          <div class="pitem"><div class="pname" data-i18n="coldRecompute">冷重算次数</div><div class="pval"><span id="pv-cold">—</span><span id="pv-cold-d" class="yellow"></span></div></div>
          <div class="pitem"><div class="pname" data-i18n="heldSessions">占位会话数</div><div class="pval" id="pv-held">—</div></div>
          <div class="pitem"><div class="pname" data-i18n="snapReloc">快照搬移次数</div><div class="pval"><span id="pv-loc">—</span><span id="pv-loc-d" class="yellow"></span></div></div>
          <div class="pitem"><div class="pname" data-i18n="defrag">碎片整理次数</div><div class="pval" id="pv-packed">—</div></div>
          <div class="pitem"><div class="pname" data-i18n="reservedCompress">压缩预留次数</div><div class="pval" id="pv-clamped">—</div></div>
        </div>
      </div>
    </section>

    <section class="bento bot">
      <div class="card" id="c-pc">
        <h2><span data-i18n="promptCache">Prompt 缓存</span>
          <button class="qh" type="button" tabindex="0" aria-label="Prompt 缓存说明" data-i18n-aria="promptCacheAria" data-tip="Prompt 缓存与内存全景中的「磁盘读缓存」不同：前者是 halogen 的会话前缀缓存（位于进程内存中），后者是 Linux 内核的磁盘读缓存（主要为模型查找表）" data-i18n-tip="promptCacheTip">?</button>
          <span class="badge miss" id="pc-miss" style="display:none" data-i18n="missCache">cache 数据缺失</span></h2>
        <div class="stat-row">
          <div class="stat g"><div class="v" id="pc-hit"><span class="skel sm"></span></div><div class="lbl" data-i18n="hitRate">命中率</div></div>
          <div class="stat g"><div class="v" id="pc-thit"><span class="skel sm"></span></div><div class="lbl" data-i18n="tokenHitRate">Token 命中率</div></div>
          <div class="stat"><div class="v" id="pc-size"><span class="skel sm"></span></div><div class="lbl" data-i18n="cacheSize">缓存大小</div></div>
          <div class="stat o" id="pc-ent-s"><div class="v" id="pc-ent"><span class="skel sm"></span></div><div class="lbl" data-i18n="cacheEntries">缓存条目数</div></div>
        </div>
        <div class="sparkgrp">
          <div class="sparkbox" id="sb-pc">
            <svg class="spark" viewBox="0 0 480 72" preserveAspectRatio="none" aria-hidden="true"><defs><linearGradient id="gsp-pc" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="var(--accent)" stop-opacity=".25"/><stop offset="1" stop-color="var(--accent)" stop-opacity="0"/></linearGradient></defs><path class="sa" fill="url(#gsp-pc)" stroke="none"/><path class="sl" fill="none" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/><circle class="se" r="0"/><line class="cr" y1="0" y2="72" style="display:none" stroke="var(--border-strong)" stroke-width="1" vector-effect="non-scaling-stroke"/></svg>
            <span class="stime l" id="st-pc0"></span><span class="stime r" id="st-pc1"></span>
            <div class="sp-empty" id="sbe-pc" style="display:none"><span class="breath"></span><span id="sbe-pc-t" data-i18n="waitingData">等待数据…</span></div>
            <div class="sparktip"></div>
          </div>
        </div>
      </div>
      <div class="card" id="c-pano">
        <h2><span data-i18n="memPano">内存全景</span>
          <button class="qh" type="button" tabindex="0" aria-label="内存全景说明" data-i18n-aria="memPanoAria" data-tip="六段按占用者划分，合计 = 总内存&#10;磁盘读缓存：内核文件读缓存，可随时回收，不构成负载&#10;主机内存「可用」长期保持 16–20 GiB，为引擎运行安全余量（MemAvailable 低于 16 GiB 时引擎拒绝启动），并非闲置浪费" data-i18n-tip="memPanoTip">?</button>
          <span class="badge miss" id="pano-miss" style="display:none" data-i18n="missMem">内存数据缺失</span></h2>
        <div class="donut" id="pano">
          <div class="prwrap">
            <svg viewBox="0 0 200 200" aria-hidden="true"><g id="pn-ticks"></g></svg>
            <div class="donut-center"><div class="dn-num"><span id="donut-total">—</span> <span style="font-size:11px;font-weight:600">GiB</span></div><div class="dn-sub" data-i18n="dnSub">= GTT 内存</div></div>
          </div>
        </div>
        <div class="legend" id="pano-legend">
          <span class="lg-item" id="lg-eng-s"><i style="background:var(--pano-eng)"></i><span data-i18n="panoEng">推理引擎（模型+KV 缓存池）</span> <b id="lg-eng">—</b></span>
          <span class="lg-item" id="lg-proc-s"><i style="background:var(--pano-proc)"></i><span data-i18n="panoProc">其他进程</span> <b id="lg-proc">—</b></span>
          <span class="lg-item" id="lg-kern-s"><i style="background:var(--pano-kern)"></i><span data-i18n="panoKern">内核占用</span> <b id="lg-kern">—</b></span>
          <span class="lg-item" id="lg-read-s"><i style="background:var(--pano-read)"></i><span data-i18n="panoRead">磁盘读缓存（可回收）</span> <b id="lg-read">—</b></span>
          <span class="lg-item" id="lg-free-s"><i style="background:var(--pano-free)"></i><span data-i18n="panoFree">空闲</span> <b id="lg-free">—</b></span>
          <span class="lg-item" id="lg-pin-s"><i style="background:var(--pano-pin)"></i><span data-i18n="panoPin">其他驻留</span> <b id="lg-pin">—</b></span>
        </div>
      </div>
    </section>
    <div class="foot" id="foot"><svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6.4"/><path d="M8 7.4v3.6M8 4.9v.2"/></svg><span id="foot-t"></span></div>
  </div>
</main>
</div>
<script>
function $(id){return document.getElementById(id);}
function isNum(x){return typeof x==='number'&&isFinite(x);}
function num(x,d){if(!isNum(x))return '—';
  if(d==null)return Math.round(x).toLocaleString('en-US');return x.toFixed(d);}
var F0=function(x){return num(x,0);};
var GiB=function(x){return (x/1073741824).toFixed(1)+' GiB';};
var HK=['ts','in_flight','queued','kv_cache_usage_ratio','cache_bytes',
        'predicted_tokens_seconds','decode_total_tps','gpu_busy_percent','gpu_temp_c','gpu_power_w',
        'ev_relocated','ev_cold_resorts'];
var hist=[],lastData=null,es=null,pollTimer=null,sseErr=0;
var SPD_CAP=72;  /* w13-ⓐ：与后端 DECODE_TPS_CAP 同值；w15-合规P1-2：seed 从 /data.json 顶层 decode_cap 取后端实际值（缺省回退 72）；history 读时过滤 >cap 的陈旧尖峰（30m 峰值+迷你图+走势都跳过） */
function capRow(r){if(r&&isNum(r.decode_total_tps)&&r.decode_total_tps>SPD_CAP){r=Object.assign({},r,{decode_total_tps:null});}return r;}
var cacheHold={};
var queuedHold=0,pkHold=null;  /* w10-ⓐ 排队告警保持 / w11-ⓝ 30m 峰值保持（单流副值已删） */
var DEMO=(new URLSearchParams(location.search)).get('demo');
function guideShort(s){if(!isNum(s.ts))return t('collecting');return s.engine_sleeping?t('shortSleep'):t('shortOff');}
function show(el,on){el.style.display=on?'':'none';}
function txt(el,v){el.classList.remove('skel','lg','sm','sk-kpi','gsk');el.textContent=v;}
function sk(el,w){el.textContent='';el.classList.remove('lg','sm','sk-kpi','gsk');el.classList.add('skel');if(w)el.classList.add(w);}
/* w9-I：采集异常（键缺失/err 键/NaN）时数值保持——每个显示位记住最后有效值，原位保留不空白；恢复后新值顶掉旧值 */
var lastGood={};
function hv(k,ok,disp,el,w){
  if(ok)lastGood[k]=disp;
  var v=(k in lastGood)?lastGood[k]:null;
  if(v===null){sk(el,w);}else{txt(el,v);}}
function pvSet(el,x,cls,fmt){
  var t=isNum(x)?(fmt?fmt(x):num(x)):((el.id in lastGood)?lastGood[el.id]:null);
  if(isNum(x))lastGood[el.id]=t;
  var want=(cls?cls+' ':'')+(t===null?'skel sm':'');
  if(el.className!==want)el.className=want;
  var v=t===null?'':t;
  if(el.textContent!==v)el.textContent=v;}
function sumLast(H,k,n){var t=0;for(var i=Math.max(0,H.length-n);i<H.length;i++){var x=H[i][k];if(isNum(x))t+=x;}return t;}
/* ---------- 自绘 tooltip（禁原生 title；hover+focus 触发，JS 定位不越视口） ---------- */
var tipEl=$('tip');
function showTip(el){var t=el.getAttribute('data-tip');if(!t)return;
  tipEl.textContent=t;tipEl.style.display='block';
  var r=el.getBoundingClientRect(),tr=tipEl.getBoundingClientRect();
  var top=r.top-tr.height-8;
  if(top<4)top=r.bottom+8;
  if(top+tr.height>window.innerHeight-4)top=Math.max(4,window.innerHeight-tr.height-4);
  var left=r.left+r.width/2-tr.width/2;
  left=Math.max(4,Math.min(left,window.innerWidth-tr.width-4));
  tipEl.style.top=top+'px';tipEl.style.left=left+'px';}
function hideTip(){tipEl.style.display='none';}
document.addEventListener('mouseover',function(e){var el=e.target.closest&&e.target.closest('[data-tip]');if(el)showTip(el);});
document.addEventListener('mouseout',function(e){var el=e.target.closest&&e.target.closest('[data-tip]');if(el)hideTip();});
document.addEventListener('focusin',function(e){var el=e.target.closest&&e.target.closest('[data-tip]');if(el)showTip(el);});
document.addEventListener('focusout',function(){hideTip();});
window.addEventListener('scroll',function(){hideTip();},true);
/* ---------- sparkline（图内两端 HH:MM:SS，图外零小字） ---------- */
var SP={},EMA_A=0.15,SP_GAP=4;  /* w14-ⓨ 方案A：α 0.3→0.15 强平滑（1800 点密度下尖峰仍可见、仅峰顶微钝） */
/* A3：单调三次插值（Fritsch–Carlson，防过冲）→ 平滑贝塞尔，无直角折线 */
function smoothD(pts){
  var n=pts.length;
  if(n<2)return'';
  if(n===2)return'M'+pts[0][0].toFixed(1)+' '+pts[0][1].toFixed(1)+'L'+pts[1][0].toFixed(1)+' '+pts[1][1].toFixed(1);
  var dx=[],m=[],t=[],i;
  for(i=0;i<n-1;i++){dx[i]=pts[i+1][0]-pts[i][0];m[i]=(pts[i+1][1]-pts[i][1])/(dx[i]||1);}
  t[0]=m[0];t[n-1]=m[n-2];
  for(i=1;i<n-1;i++)t[i]=(m[i-1]*m[i]<=0)?0:(m[i-1]+m[i])/2;
  for(i=0;i<n-1;i++){
    if(m[i]===0){t[i]=0;t[i+1]=0;}
    else{var a=t[i]/m[i],b=t[i+1]/m[i],c=a*a+b*b;
      if(c>9){var k=3/Math.sqrt(c);t[i]=k*a*m[i];t[i+1]=k*b*m[i];}}}
  var d='M'+pts[0][0].toFixed(1)+' '+pts[0][1].toFixed(1);
  for(i=0;i<n-1;i++){
    var x0=pts[i][0],y0=pts[i][1],x1=pts[i+1][0],y1=pts[i+1][1],s3=dx[i]/3;
    d+='C'+(x0+s3).toFixed(1)+' '+(y0+t[i]*s3).toFixed(1)+','
      +(x1-s3).toFixed(1)+' '+(y1-t[i+1]*s3).toFixed(1)+','
      +x1.toFixed(1)+' '+y1.toFixed(1);}
  return d;}
function makeSpark(box,fmt,color,h){
  h=h||32;
  var svg=box.querySelector('svg.spark'),tip=box.querySelector('.sparktip');
  var o={svg:svg,tip:tip,fmt:fmt,vals:[],h:h,path:svg.querySelector('.sl'),
         area:svg.querySelector('.sa'),end:svg.querySelector('.se'),cross:svg.querySelector('.cr')};
  o.path.style.stroke=color;o.end.style.fill=color;
  o.update=function(vals){
    o.vals=vals||[];var v=o.vals.filter(isNum);
    if(v.length<2){o.path.setAttribute('d','');o.area.setAttribute('d','');o.end.setAttribute('r','0');return;}
    /* w14-ⓨ 方案A：EMA（α=0.15）强平滑后作图（w10-ⓗ 基础上加大力度）；tooltip 保留原始值 */
    var sv=[],ema=null,i0;
    for(i0=0;i0<o.vals.length;i0++){var x0=o.vals[i0];
      if(!isNum(x0)){sv.push(null);continue;}
      ema=(ema===null)?x0:EMA_A*x0+(1-EMA_A)*ema;sv.push(ema);}
    var vs=sv.filter(isNum);
    var mn=Math.min.apply(null,vs),mx=Math.max.apply(null,vs);
    if(mx-mn<1e-9){mn=mn-0.5;mx=mn+1;}
    var n=o.vals.length,pts=[],last=null;
    /* w12-ⓖ：折线最低点↔时间标签间隙 = 固定像素（标签高+SP_GAP），两图共用逻辑 → 间隙像素相等 */
    var Hpx=svg.getBoundingClientRect().height||h;
    var lab=box.querySelector('.stime');
    var labH=(lab&&lab.getBoundingClientRect().height)||18;
    var bot=(h-(labH+SP_GAP)*h/Hpx);if(bot<20)bot=20;o.bot=bot;
    for(var i=0;i<n;i++){var x=sv[i];if(!isNum(x))continue;
      var px=(i/(n-1))*478+1,py=bot-((x-mn)/(mx-mn))*(bot-8);
      pts.push([px,py]);last=[px,py];}
    var d=smoothD(pts);
    o.path.setAttribute('d',d);
    o.area.setAttribute('d',d+'L'+last[0].toFixed(1)+' '+h+'L'+pts[0][0].toFixed(1)+' '+h+'Z');
    o.end.setAttribute('cx',last[0].toFixed(1));o.end.setAttribute('cy',last[1].toFixed(1));
    o.end.setAttribute('r','3');};
  svg.addEventListener('mousemove',function(e){
    var n=o.vals.length;if(n<2){o.cross.style.display='none';o.tip.style.display='none';return;}
    var rect=svg.getBoundingClientRect();
    var idx=Math.round((e.clientX-rect.left)/rect.width*(n-1));
    if(idx<0)idx=0;if(idx>=n)idx=n-1;
    var x=o.vals[idx];
    if(!isNum(x)){o.cross.style.display='none';o.tip.style.display='none';return;}
    var px=(idx/(n-1))*478+1;
    o.cross.setAttribute('x1',px);o.cross.setAttribute('x2',px);o.cross.style.display='block';
    o.tip.textContent=o.fmt(x);o.tip.style.display='block';
    var left=Math.max(32,Math.min(e.clientX-rect.left,rect.width-32));
    o.tip.style.left=left+'px';});
  svg.addEventListener('mouseleave',function(){o.cross.style.display='none';o.tip.style.display='none';});
  return o;}
SP['sb-kv']=makeSpark($('sb-kv'),function(x){return (x*100).toFixed(1)+'%';},'var(--chart)',72);
SP['sb-pc']=makeSpark($('sb-pc'),GiB,'var(--accent)',72);
function updSpark(id,H,key,t0id,t1id,emptyId,emptyTId,s){
  var o=SP[id],vals=H.map(function(x){return x[key];});
  o.update(vals);
  var f=null,l=null;
  for(var i=0;i<H.length;i++){if(isNum(H[i][key])){if(f===null)f=H[i].ts;l=H[i].ts;}}
  var fT=function(t){return new Date(t*1000).toLocaleTimeString('zh-CN',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});};
  var on=f!==null;
  $(t0id).textContent=on?fT(f):'';$(t1id).textContent=on?fT(l):'';
  show($(emptyId),!on);if(!on)$(emptyTId).textContent=guideShort(s);}
/* ---------- KPI mini 柱（16 根，柱高=区间最大值归一，柱色随三段） ---------- */
var MB={};
(function(){var ids=['spd','gpu','temp','pow'];
  for(var q=0;q<ids.length;q++){var el=$('mb-'+ids[q]),kids=[];
    for(var i=0;i<16;i++){var b=document.createElement('i');el.appendChild(b);kids.push(b);}
    MB[ids[q]]=kids;}})();
function updMini(id,H,key){
  var kids=MB[id],el=$('mb-'+id),n=H.length,bk=[],hasNum=false;
  var per=Math.max(1,Math.floor(n/16)),start=Math.max(0,n-16*per);
  for(var b=0;b<16;b++){
    var mx=null;
    for(var j=start+b*per;j<start+(b+1)*per&&j<n;j++){
      var x=H[j][key];
      if(isNum(x)&&(mx===null||x>mx))mx=x;}
    bk.push(mx);if(mx!==null)hasNum=true;}
  el.classList.toggle('wait',!hasNum);
  if(!hasNum)return;
  var top=0;
  for(var k=0;k<16;k++)if(isNum(bk[k])&&bk[k]>top)top=bk[k];
  for(var k2=0;k2<16;k2++){var v=bk[k2];
    kids[k2].style.height=(v===null||v<=0||top<=0)?'2px':Math.max(2,Math.round(v/top*100))+'%';}}
/* ---------- 三段变色（只变色不写字；w11-ⓕ2 阈值按总吞吐口径重定标：单流均值 32.1×3 槽满载≈96→100） ---------- */
function segSpd(v){return v<=0?'seg-idle':(v<100?'seg-norm':'seg-high');}
/* F3：cache 通道异常时池压力 7 项保持上一次值（慢刷数据本就变得慢），来新数据才更新 */
function cval(s,k){var v=s[k];
  if(isNum(v)){cacheHold[k]=v;return v;}
  return (k in cacheHold)?cacheHold[k]:v;}
function segBusy(v){return v<5?'seg-idle':(v<=85?'seg-norm':'seg-high');}
function segTemp(v){return v<55?'seg-idle':(v<=75?'seg-norm':'seg-high');}
function segPow(v){return v<40?'seg-idle':(v<=95?'seg-norm':'seg-high');}
function kpiCard(id,v,fmt,seg,H,key){
  var card=$('kc-'+id),vEl=$('k-'+id+'-v'),uEl=$('k-'+id+'-u');
  var fresh=isNum(v);
  if(!fresh&&('kpi-'+id in lastGood))v=lastGood['kpi-'+id];  /* w9-I：断供保持最后有效值 */
  if(!isNum(v)){sk(vEl,'sk-kpi');show(uEl,false);card.className='card kpi';
    $('mb-'+id).classList.add('wait');return;}
  if(fresh)lastGood['kpi-'+id]=v;
  txt(vEl,fmt(v));show(uEl,true);
  vEl.classList.toggle('stale',!fresh);  /* w10-ⓖ：断供保持+置灰，不归零 */
  card.className='card kpi '+seg(v);
  updMini(id,H,key);}
/* ---------- 扇叶仪表（A1 恢复 v17 放射刻度扇；A9 三段阈值色；值变化 rAF 平滑过渡） ---------- */
var GT=[],gShown=0,gAnim=0;
(function(){var g=$('g-ticks');
  for(var i=0;i<41;i++){var ln=document.createElementNS('http://www.w3.org/2000/svg','line');
    ln.setAttribute('x1','100');ln.setAttribute('y1','8');ln.setAttribute('x2','100');ln.setAttribute('y2','28');
    ln.setAttribute('transform','rotate('+(-120+i*6)+' 100 100)');ln.setAttribute('class','tk');
    g.appendChild(ln);GT.push(ln);}})();
function gaugeSeg(r){return r<0.5?'seg-idle':(r<=0.95?'seg-norm':'seg-high');}
function setGauge(r){var on=Math.round(r*41);
  for(var i=0;i<41;i++)GT[i].classList.toggle('on',i<on);
  txt($('g-num'),(r*100).toFixed(1));}
function tweenGauge(target){
  cancelAnimationFrame(gAnim);
  var from=gShown,t0=performance.now(),D=800;
  function step(t){var p=Math.min(1,(t-t0)/D),e=1-Math.pow(1-p,3);
    gShown=from+(target-from)*e;setGauge(gShown);
    if(p<1)gAnim=requestAnimationFrame(step);else{gShown=target;setGauge(target);}}
  gAnim=requestAnimationFrame(step);}
function updGauge(s){
  var r=isNum(s.kv_cache_usage_ratio)?Math.min(Math.max(s.kv_cache_usage_ratio,0),1):null;
  var wrap=$('g-wrap');
  if(r===null&&('gauge' in lastGood))r=lastGood['gauge'];  /* w9-I：断供保持最后有效值 */
  if(r===null){sk($('g-num'),'gsk');show($('g-unit'),false);
    cancelAnimationFrame(gAnim);gShown=0;wrap.className='gwrap';
    for(var qi=0;qi<41;qi++)GT[qi].classList.remove('on');return;}
  lastGood['gauge']=r;
  show($('g-unit'),true);
  wrap.className='gwrap '+gaugeSeg(r);
  tweenGauge(r);}
/* ---------- w12-ⓑ：仪表大数字按字形墨迹盒光学居中 ----------
   数字无下伸部（descender）：按 line-box 居中时墨迹视觉偏上。
   canvas measureText 实测数字墨迹盒（actualBoundingBoxAscent/Descent），
   求墨迹中心与行盒中心偏移 dy，注入 --g-ink-dy 下移补偿；字体加载后重测。 */
function gaugeInkCenter(){
  var gn=document.querySelector('.g-num');if(!gn)return;
  var cs=getComputedStyle(gn);
  var ctx=gaugeInkCenter.c||(gaugeInkCenter.c=document.createElement('canvas').getContext('2d'));
  ctx.font=cs.fontWeight+' '+cs.fontSize+' '+cs.fontFamily;
  var m=ctx.measureText('0');
  var aA=m.actualBoundingBoxAscent,aD=m.actualBoundingBoxDescent;
  if(!(aA>0))return;
  var fs=parseFloat(cs.fontSize);
  var lh=parseFloat(cs.lineHeight);if(!isFinite(lh))lh=fs*1.5;
  var fA=m.fontBoundingBoxAscent,fD=m.fontBoundingBoxDescent;
  if(!(fA>0)){fA=fs;fD=fs*0.25;}
  var halfLead=Math.max(0,(lh-(fA+fD))/2);
  var baseline=halfLead+fA;
  var dy=lh/2-(baseline-(aA+aD)/2);
  document.documentElement.style.setProperty('--g-ink-dy',dy.toFixed(2)+'px');
}
if(document.fonts&&document.fonts.ready)document.fonts.ready.then(gaugeInkCenter);
/* ---------- 侧栏槽位（自适应 total_slots，明细表已删） ---------- */
function mkSideRow(){var d=document.createElement('div');d.className='slot-li';
  var dot=document.createElement('span');dot.className='dot idle';
  var nm=document.createElement('span');
  var rt=document.createElement('span');rt.className='idle-t';
  d.appendChild(dot);d.appendChild(nm);d.appendChild(rt);return d;}
function updSlots(s){
  var sd=s.slots_detail||[];
  var n=isNum(s.total_slots)?s.total_slots:(isNum(s.slots)?s.slots:0);
  var engDown=!isNum(s.ts)||!s.halogen_ok;
  var count=sd.length?sd.length:(isNum(s.ts)?n:0);
  count=Math.min(count,1024);  /* w15-bugP1-4：钳制，⛔ 引擎畸形 total_slots 打挂页签 */
  var side=$('side-slots');
  if(count===0&&isNum(s.ts)){
    if(side.children.length!==1||!side.children[0].dataset.ph){
      side.textContent='';
      var li=document.createElement('div');li.className='slot-li';li.dataset.ph='1';
      var bd=document.createElement('span');bd.className='breath';
      var bt=document.createElement('span');bt.style.color='var(--muted)';
      li.appendChild(bd);li.appendChild(bt);side.appendChild(li);}
    side.querySelector('[data-ph] span:last-child').textContent=t('noSlotData');
    return;}
  if(side.children.length&&side.children[0].dataset.ph)side.textContent='';
  while(side.children.length>count)side.removeChild(side.lastChild);
  while(side.children.length<count)side.appendChild(mkSideRow());
  for(var i=0;i<count;i++){
    var it=sd[i]||null;
    var busy=it&&it.state==='busy';
    var sr=side.children[i];
    sr.children[0].className='dot '+(engDown?'wait':busy?'busy':'idle');
    /* w13-ⓑ：展示编号从 1 起（后端 id/数据键仍从 0 起）；w13-ⓒ→w17-②：数字包 .slot-n=mono 等宽族，中文保持正文族 */
    var idx=(it?it.id:i)+1;
    var nm=sr.children[1];var pp=t('slotN',{i:'\u0001'}).split('\u0001');
    var ns=nm.querySelector('.slot-n');
    if(!ns){ns=document.createElement('span');ns.className='slot-n';nm.textContent='';
      nm.appendChild(document.createTextNode(pp[0]));nm.appendChild(ns);nm.appendChild(document.createTextNode(pp[1]||''));}
    else{nm.firstChild.textContent=pp[0];if(nm.lastChild!==ns)nm.lastChild.textContent=pp[1]||'';}
    ns.textContent=idx;
    var spd=(it&&isNum(it.decode_ts))?it.decode_ts.toFixed(1)+' tok/s':null;
    if(spd){sr.children[2].className='spd';sr.children[2].textContent=spd;}
    else{sr.children[2].className='idle-t';
      sr.children[2].textContent=engDown?(s.engine_sleeping?t('shortSleep'):t('slotOffline')):(busy?t('slotGenerating'):t('slotIdle'));}}}
/* ---------- 内存全景（w9-E：放射刻度环，60 根 ×6°=360°，刻度几何/粗细同占用率仪表 .tk；w9-I：断供保持） ---------- */
var PANO=[['eng','pano_engine_gib'],['proc','pano_procs_gib'],['kern','pano_kernel_gib'],
          ['read','pano_readcache_gib'],['free','pano_free_gib'],['pin','pano_pinned_gib']];
var PT=[];
(function(){var g=$('pn-ticks');
  for(var i=0;i<60;i++){var ln=document.createElementNS('http://www.w3.org/2000/svg','line');
    ln.setAttribute('x1','100');ln.setAttribute('y1','8');ln.setAttribute('x2','100');ln.setAttribute('y2','28');
    ln.setAttribute('transform','rotate('+(i*6)+' 100 100)');ln.setAttribute('class','tk');
    g.appendChild(ln);PT.push(ln);}})();
function updPano(s){
  var mt=isNum(s.mem_total_gib)?s.mem_total_gib:null;
  var ok=!!(mt&&isNum(s.pano_engine_gib));
  show($('pano-miss'),isNum(s.ts)&&!ok);
  hv('donut-total',ok,num(mt,1),$('donut-total'),'sm');
  var gtt=isNum(s.gtt_used_gib)?num(s.gtt_used_gib,1):'—';
  var mav=isNum(s.mem_available_gib)?num(s.mem_available_gib,1):'—';
  var tips={eng:t('tipEng',{gtt:gtt}),
    proc:t('tipProc'),
    kern:t('tipKern'),
    read:t('tipRead'),
    free:t('tipFree',{mav:mav}),
    pin:t('tipPin')};
  var fr=[];
  if(ok){
    var sum=0;
    for(var i=0;i<PANO.length;i++){var f=isNum(s[PANO[i][1]])?Math.max(0,Math.min(s[PANO[i][1]]/mt,1)):0;fr.push(f);sum+=f;}
    if(sum>0)for(var n=0;n<fr.length;n++)fr[n]/=sum;
    lastGood['pano-fr']=fr;
  }else if('pano-fr' in lastGood){fr=lastGood['pano-fr'];}
  var starts=[],acc=0;
  for(var m=0;m<PANO.length;m++){starts.push(acc);acc+=fr[m];}
  for(var q=0;q<60;q++){
    var a=(q+0.5)/60,cls='tk',key=null;
    for(var j=0;j<PANO.length;j++){if(a>=starts[j]&&a<starts[j]+fr[j]){cls='tk pk-'+PANO[j][0];key=PANO[j][0];break;}}
    var ln=PT[q];
    if(ln.getAttribute('class')!==cls)ln.setAttribute('class',cls);
    if(key)ln.setAttribute('data-tip',tips[key]);
    else if(ln.hasAttribute('data-tip'))ln.removeAttribute('data-tip');}
  for(var p=0;p<PANO.length;p++){
    var k=PANO[p][0],v=s[PANO[p][1]],lg=$('lg-'+k),lgs=$('lg-'+k+'-s');
    hv('lg-'+k,isNum(v),num(v,1),lg,'sm');
    lgs.setAttribute('data-tip',tips[k]);}}
/* ---------- 主渲染 ---------- */
function render(s){
  lastData=s;
  if(DEMO==='empty')s={ts:Date.now()/1000,halogen_ok:false,err_health:'demo',err_metrics:'demo',err_cache:'demo'};
  if(DEMO==='sleep')s={ts:Date.now()/1000,halogen_ok:true,engine_sleeping:true};
  var H=DEMO?[]:hist;
  var first=!isNum(s.ts);
  var sleeping=!!s.engine_sleeping;
  var engDown=!first&&!s.halogen_ok;
  /* 顶栏：状态 pill（承载主提示）+ 更新时间 */
  var dot=$('dot'),pill=$('status-pill'),stxt=$('status-txt');
  if(first){dot.className='dot wait';stxt.textContent=t('connecting');pill.removeAttribute('data-tip');}
  else if(sleeping){dot.className='dot sleep';stxt.textContent=t('modelSleeping');pill.setAttribute('data-tip',t('guideSleep'));}
  else if(s.halogen_ok){dot.className='dot on';stxt.textContent=t('engineRunning');pill.removeAttribute('data-tip');}
  else{dot.className='dot off';stxt.textContent=t('engineNotResp');pill.setAttribute('data-tip',t('guideOff'));}
  $('sub').textContent=t('lastUpdated',{t:(first?'—':new Date(s.ts*1000).toLocaleTimeString('zh-CN',{hour12:false}))});
  /* 侧栏模型信息（w9-I：hv 保持最后有效值） */
  var mName=$('m-name');
  var mAlias=s.model_alias||s.model;
  hv('m-name',!!mAlias,mAlias||'',mName);
  mName.setAttribute('title',mAlias||'');  /* w10-ⓙP2-1：单行截断后悬浮全名 */
  hv('m-ctx',isNum(s.model_context),num(s.model_context),$('m-ctx'),'sm');
  hv('m-kv',!!s.kv_cache_type,s.kv_cache_type?s.kv_cache_type.toUpperCase():'',$('m-kv'),'sm');
  hv('m-fmt',!!s.checkpoint_format,s.checkpoint_format?s.checkpoint_format.toUpperCase():'',$('m-fmt'),'sm');
  hv('m-ver',!!s.engine_version,s.engine_version?'v'+s.engine_version:'',$('m-ver'),'sm');
  var nSlots=isNum(s.total_slots)?s.total_slots:(isNum(s.slots)?s.slots:null);
  hv('m-slots',nSlots!=null,num(nSlots)||'',$('m-slots'),'sm');
  /* 采集健康（侧栏）：err_* 键即通道健康状态（后端已"判异常前先重试一次"） */
  var HN=[['health','err_health'],['metrics','err_metrics'],['cache','err_cache'],['props','err_props']];
  for(var hz=0;hz<HN.length;hz++){
    var hBad=!!s[HN[hz][1]],hDot=$('h-'+HN[hz][0]),hSt=$('hs-'+HN[hz][0]);
    hDot.className='dot '+(first?'wait':hBad?'h-bad':'h-ok');
    var hTxt=first?t('collecting'):(hBad?t('healthErr'):t('healthOk'));
    if(hSt.textContent!==hTxt)hSt.textContent=hTxt;
    var hCls='hst'+(hBad&&!first?' bad':'');
    if(hSt.className!==hCls)hSt.className=hCls;}
  updSlots(s);
  /* 侧栏 在途/排队 + 等待/放弃（w9-I：保持） */
  function qSet(el,v){hv(el.id,isNum(v),num(v),el,'sm');}
  qSet($('q-flight'),s.in_flight);qSet($('q-queued'),s.queued);
  qSet($('q-waited'),s.queue_waited_total);qSet($('q-gave'),s.queue_gave_up_total);
  /* w10-ⓐ：queued>0 → 在途请求块告警呼吸 + 顶栏 h1 右侧全局呼吸红点（断供时保持最后状态） */
  if(isNum(s.queued))queuedHold=s.queued;
  var qAlert=queuedHold>0;
  $('q-block').classList.toggle('alert',qAlert);
  $('h1-alert').style.display=qAlert?'':'none';
  $('q-wait-row').setAttribute('data-tip',
    isNum(s.queue_waited_total)
      ?t('waitTipKnown',{n:num(s.queue_waited_total),s:(isNum(s.queue_wait_seconds_total)?num(s.queue_wait_seconds_total,1):'—')})
      :t('waitTipUnknown'));
  /* KPI 四卡（w11-ⓕ2：解码速度主值=在途请求数×最近上报单流速度（后端实时估算）；w11-ⓝ：副值只剩 30m 峰值） */
  kpiCard('spd',s.decode_total_tps,function(x){return x.toFixed(1);},segSpd,H,'decode_total_tps');
  var pkEl=$('k-spd-p');
  var pk=null;
  for(var pi=Math.max(0,H.length-1800);pi<H.length;pi++){var pv=H[pi].decode_total_tps;if(isNum(pv)&&(pk===null||pv>pk))pk=pv;}
  if(pk!==null)pkHold=pk;
  pkEl.textContent=(pkHold!==null)?pkHold.toFixed(1):'—';
  pkEl.classList.toggle('stale',!isNum(s.decode_total_tps));  /* w10-ⓖ */
  kpiCard('gpu',s.gpu_busy_percent,F0,segBusy,H,'gpu_busy_percent');
  kpiCard('temp',s.gpu_temp_c,F0,segTemp,H,'gpu_temp_c');
  kpiCard('pow',s.gpu_power_w,F0,segPow,H,'gpu_power_w');
  /* KV 水位：扇形 + 扇下 已用/预留/总 + 走势 + 底部统计（池满风险文案已删，仅真排队亮 badge） */
  updGauge(s);
  function uSet(el,v){hv(el.id,isNum(v),num(v),el,'sm');}
  uSet($('kv-used-t'),s.kv_cache_tokens);uSet($('kv-used-r'),s.kv_pool_reserved_tokens);
  uSet($('kv-used-p'),s.kv_pool_positions);
  updSpark('sb-kv',H,'kv_cache_usage_ratio','st-kv0','st-kv1','sbe-kv','sbe-kv-t',s);
  var r=isNum(s.kv_cache_usage_ratio)?s.kv_cache_usage_ratio:null;
  var wKnown=isNum(s.kv_pool_waiting_for_room);
  var w=(s.kv_pool_waiting_for_room||0)>0;
  show($('kv-badge'),w);
  show($('kv-sleep'),sleeping);
  $('kv-miss').textContent=first?t('collecting'):t('missMetrics');
  show($('kv-miss'),!first&&r==null);
  $('c-kv').classList.toggle('alert',!!w);
  var kq=$('kv-queue');
  if(!wKnown){  /* w9-I：断供保持最后排队状态（卡头 miss 徽标与采集健康区照常如实显示） */
    var hq=lastGood['kv-queue'];
    if(hq){kq.className='badge '+hq.c;kq.textContent=t(hq.k);}
    else{kq.className='badge miss';kq.textContent=first?t('collecting'):t('dataMissing');}}
  else if(w){kq.className='badge bad';kq.textContent=t('queuing');lastGood['kv-queue']={c:'bad',k:'queuing'};}
  else{kq.className='badge up';kq.textContent=t('noQueue');lastGood['kv-queue']={c:'up',k:'noQueue'};}
  hv('kv-draft',isNum(s.draft_accept_rate),isNum(s.draft_accept_rate)?(s.draft_accept_rate*100).toFixed(0)+'%':'',$('kv-draft'),'sm');
  hv('kv-req',isNum(s.requests_total),num(s.requests_total),$('kv-req'),'sm');
  /* KV 缓存池压力（7 项升序；池水位 badge 已删；pdesc 全部进标题 [?] tooltip） */
  var cKnown=isNum(s.cache_waiting_for_room);
  var pw=(cval(s,'cache_waiting_for_room')||0)>0;
  $('c-pool').classList.toggle('alert',!!pw);
  show($('pool-badge'),pw);
  $('pool-miss').textContent=first?t('collecting'):t('missCache');
  show($('pool-miss'),!cKnown);
  pvSet($('pv-waiting'),cval(s,'cache_waiting_for_room'),pw?'pval red':'pval');
  pvSet($('pv-waitings'),cval(s,'cache_waiting_s'),'pval',function(x){return num(x,1)+t('secUnit');});
  var evR=sumLast(H,'ev_relocated',60),evC=sumLast(H,'ev_cold_resorts',60);
  pvSet($('pv-cold'),cval(s,'cache_cold_resorts'),evC>0?'yellow':'');
  $('pv-cold-d').textContent=evC>0?' (+'+evC+')':'';
  pvSet($('pv-held'),cval(s,'cache_held_regions'),'pval');
  pvSet($('pv-loc'),cval(s,'cache_relocated'),'pval');
  $('pv-loc-d').textContent=evR>0?' (+'+evR+')':'';
  pvSet($('pv-packed'),cval(s,'cache_packed'),'pval');
  pvSet($('pv-clamped'),cval(s,'cache_room_clamped'),'pval');
  /* Prompt 缓存：命中/token/大小/条目 + 走势 */
  $('pc-miss').textContent=first?t('collecting'):t('missCache');
  show($('pc-miss'),!!s.err_cache&&!first);
  hv('pc-hit',isNum(s.cache_hit_rate),isNum(s.cache_hit_rate)?(s.cache_hit_rate*100).toFixed(1)+'%':'',$('pc-hit'),'sm');
  hv('pc-thit',isNum(s.cache_token_hit_rate),isNum(s.cache_token_hit_rate)?(s.cache_token_hit_rate*100).toFixed(1)+'%':'',$('pc-thit'),'sm');
  hv('pc-size',isNum(s.cache_bytes),isNum(s.cache_bytes)?(s.cache_bytes/1073741824).toFixed(1)+' GiB':'',$('pc-size'),'sm');
  hv('pc-ent',isNum(s.cache_entries)&&isNum(s.cache_max_entries),isNum(s.cache_entries)&&isNum(s.cache_max_entries)?num(s.cache_entries)+'/'+num(s.cache_max_entries):'',$('pc-ent'),'sm');
  $('pc-ent-s').setAttribute('data-tip',
    isNum(s.cache_superseded)
      ?t('entTipKnown',{n:num(s.cache_superseded)})
      :t('entTipUnknown'));
  updSpark('sb-pc',H,'cache_bytes','st-pc0','st-pc1','sbe-pc','sbe-pc-t',s);
  /* 内存全景 */
  updPano(s);
  /* 页脚 */
  var iv=1,wm=30;
  if(H.length>2){var gs=0,gc=0;for(var hi=1;hi<H.length;hi++){var g=H[hi].ts-H[hi-1].ts;if(g>0&&g<60){gs+=g;gc++;}}
    if(gc>0){iv=gs/gc;wm=Math.max(0,Math.round((H[H.length-1].ts-H[0].ts)/60));}}  /* w15-bugP1-5：wm 钳非负，时间回跳不显负窗口 */
  /* w10-ⓔ：异常提示纯中文通道名（内部键名/端点路径不再出现在用户文案） */
  var em=[['err_health','probeLiveness'],['err_metrics','probeMetrics'],['err_cache','probeCache'],['err_props','probeProps'],['err_meminfo','probeMeminfo']];
  var errs=[];for(var ei=0;ei<em.length;ei++){if(s[em[ei][0]])errs.push(t(em[ei][1]));}
  $('foot-t').textContent=t('footerMain',{iv:iv.toFixed(1),wm:wm})
    +(errs.length?t('footerErr',{errs:errs.join(t('listSep'))}):'');
}
/* ---------- 数据通道：SSE 主 + 轮询兜底 ---------- */
function pickRow(s){var o={};for(var i=0;i<HK.length;i++){var k=HK[i];if(k in s)o[k]=s[k];}return o;}
function mergeRows(rows){var lastT=hist.length?hist[hist.length-1].ts:0;
  for(var i=0;i<rows.length;i++){var r=rows[i];
    if(!r||!isNum(r.ts))continue;
    if(lastT&&r.ts<lastT-5){hist=[];lastT=0;}  /* w15-bugP1-5：ts 回退 >5s（NTP 向后校时/手动改时）→ 清空 hist 重建，⛔ 停摆等追平 */
    if(r.ts>lastT){hist.push(capRow(r));lastT=r.ts;}}  /* w13-ⓐ：入 hist 前滤 >cap 尖峰 */
  while(hist.length>1800)hist.shift();}
function frame(d){if(d.history)mergeRows(d.history);var s=d.latest||{};
  if(isNum(s.ts))mergeRows([pickRow(s)]);render(s);}
var lastFrame=0,lastTry=0,esGrace=0,missCount=0;
function seed(){return fetch('/data.json').then(function(r){return r.json();}).then(function(d){
    if(isNum(d.decode_cap)&&d.decode_cap>0)SPD_CAP=d.decode_cap;  /* w15-合规P1-2：前端 cap 跟随后端 DASH_DECODE_CAP，缺省回退 72 */
    lastFrame=Date.now();hist=(d.history||[]).slice(-1800).map(capRow);render(d.latest||{});})  /* w13-ⓐ：读时过滤 >cap 陈旧尖峰 */
  .catch(function(){$('sub').textContent=t('connRetry');$('dot').className='dot off';});}
function isPaused(){return document.body.classList.contains('paused');}
function connect(){
  if(es){es.close();es=null;}
  lastTry=Date.now();esGrace=lastTry;   /* 新建 EventSource 8s 宽限：看门狗不误杀刚连上的流 */
  try{es=new EventSource('/stream');}catch(e){return;}
  es.onopen=function(){   /* 双保险：SSE 一连上就清降级轮询；从轮询回切时重新 seed 补洞 */
    if(pollTimer){clearInterval(pollTimer);pollTimer=null;seed();}};
  es.onmessage=function(e){sseErr=0;missCount=0;lastFrame=Date.now();
    if(pollTimer){clearInterval(pollTimer);pollTimer=null;}
    try{frame(JSON.parse(e.data));}catch(err){}};
  es.onerror=function(){sseErr++;
    if(sseErr>=3&&!pollTimer&&!isPaused()){pollTimer=setInterval(seed,3000);}};}
/* 看门狗：连续 2 次检查（≥10s）无帧 → 关流降级 3s 轮询；轮询期每 15s 独立重试 SSE（移出无帧块，修复降级后永不回切） */
setInterval(function(){
  if(isPaused()||DEMO||!lastFrame)return;
  var now=Date.now();
  if(now-lastFrame>10000)missCount++;else missCount=0;
  if(missCount>=2&&now-esGrace>8000){
    if(es){es.close();es=null;}
    if(!pollTimer)pollTimer=setInterval(seed,3000);
  }
  if(pollTimer&&!es&&now-lastTry>15000)connect();
},2000);
$('pauseBtn').addEventListener('click',function(){
  var paused=document.body.classList.toggle('paused');
  this.setAttribute('aria-pressed',paused?'true':'false');
  $('pauseTxt').textContent=paused?t('resumeUpdates'):t('pauseUpdates');
  this.setAttribute('aria-label',paused?t('resumeUpdates'):t('pauseUpdates'));
  $('ic-pause').style.display=paused?'none':'';
  $('ic-play').style.display=paused?'':'none';
  if(paused){if(es){es.close();es=null;}
    if(pollTimer){clearInterval(pollTimer);pollTimer=null;}}
  else{seed();connect();}});
/* ---------- 主题（亮色默认，暗色可切） ---------- */
function applyTheme(th){
  if(th==='dark')document.documentElement.setAttribute('data-theme','dark');
  else document.documentElement.removeAttribute('data-theme');
  try{localStorage.setItem('halo-theme',th);}catch(e){}
  $('themeTxt').textContent=t(th==='dark'?'themeLight':'themeDark');
  $('ic-moon').style.display=th==='dark'?'none':'';
  $('ic-sun').style.display=th==='dark'?'':'none';}
$('themeBtn').addEventListener('click',function(){
  applyTheme(document.documentElement.getAttribute('data-theme')==='dark'?'light':'dark');});
/* ---------- i18n：zh-CN（默认）/ en / ko / de（日语按用户明令排除） ---------- */
var LANGS=['zh-CN','en','ko','de'];
var LANG_NAMES={'zh-CN':'中文','en':'English','ko':'한국어','de':'Deutsch'};
var lang='zh-CN';try{var _sl=localStorage.getItem('dash-lang');if(LANGS.indexOf(_sl)>=0)lang=_sl;}catch(e){}
var I18N={
'zh-CN':{
 title:'halogen 监控',brandSide:'halogen 监控',modelInfo:'模型信息',ctxLen:'上下文长度',
 kvPrec:'KV 缓存精度',weightFmt:'权重格式',engineVer:'引擎版本',slotCount:'槽位数',
  slotState:'槽位状态',slotStateAria:'槽位状态说明',slotStateTip:'引擎仅在推理完成时上报速度，不提供单槽位数据。展示从 1 起，后端编号从 0 起。',
   reqStatus:'请求状态',
    inFlight:'处理中',queuedReq:'排队请求',queueWaits:'排队等待',timesUnit:'次',abandoned:'已放弃',
   inFlightAria:'处理中说明',
   inFlightTip:'处理中 = 引擎正在处理的请求数；排队请求 = 等待槽位的请求数。排队等待 = 等待≥1 秒的次数；已放弃 = 客户端断连或排队超时。',
  colHealth:'采集健康',colHealthAria:'采集健康说明',
  colHealthTip:'四条采集通道（health/metrics/cache/props）本轮取数状态：绿点正常，红点异常（判异常前已自动重试）。通道异常期间，各卡片保留上一次有效数值不空白，恢复后自动更新。',
  probeLiveness:'活性探测',probeMetrics:'指标采集',probeCache:'缓存状态',probeProps:'引擎属性',probeMeminfo:'内存信息',
  tipHealth:`数据用于：顶栏引擎状态、槽位状态、KV 缓存池占用率
（槽位数 / 处理中 / 排队请求 / 总量）`,
  tipMetrics:'数据用于：KPI 四卡、KV 缓存池占用率与走势、投机接受率、侧栏处理中/排队请求/排队等待/已放弃计数',
 tipCache:'数据用于：KV 缓存池压力、Prompt 缓存',
 tipProps:'数据用于：侧栏模型信息（别名/槽位数）、待命中状态判定',
  themeAria:'切换亮色/暗色主题',themeDark:'暗色',themeLight:'亮色',overviewH1:'推理监控总览',
 connecting:'连接中…',loading:'加载中…',pauseUpdates:'暂停刷新',resumeUpdates:'恢复刷新',
 langSwitchAria:'切换语言',coreMetrics:'核心指标',
  decodeSpeed:'解码速度',decodeSpeedAria:'解码速度说明',peak30:'30m 峰值',
   decodeSpeedTip:`主值 = 总解码速度（实时估算）：在途请求数 × 最近一次上报的单流速度。引擎仅在请求完成时上报速度（生成进行中上报为 0），故在途产出按最近上报的单流速度估算；空闲显示 0。显示以本机带宽上限封顶。
  峰值 = 最近 30 分钟内总解码速度的最高值。`,
 gpuUtil:'GPU 利用率',gpuTemp:'GPU 温度',gpuPower:'GPU 功耗',gpuPowerAria:'GPU 功耗说明',
 gpuPowerTip:'均衡模式 85 W 为 LLM 推理的最佳功耗区间，120 W 性能模式对推理性能的提升仅约 2%。',
 kvUsage:'KV 缓存池占用率',kvUsageAria:'KV 缓存池占用率说明',
 kvUsageTip:`占用率 = KV 缓存池已用位置占比。
占用率 100% 不代表过载：池满时排队为 0、冷重算为 0 是常态，引擎会按设计将内存分配至饱和。
 占用率下降有两种途径：① 版本替换回收（旧版本缓存被新版本替换，代价为零）② LRU 淘汰（被移出池的会话在下次请求时重新执行一次 prefill，冷重算 +1）
 预留 = 在途请求声明的空间需求（prompt + max_tokens，含尚未生成的部分），与已用口径独立、不计入已用`,
  queuing:'缓存等待',modelSleeping:'模型待命中',missMetrics:'metrics 数据缺失',
  used:'已用',reserved:'预留',total:'总量',waitingData:'等待数据…',
  reservedTip:'预留 = 在途请求声明的空间需求（prompt + max_tokens，含尚未生成的部分），与已用口径独立、不计入已用',
 specAccept:'投机接受率',totalReqs:'累计请求',queueState:'排队状态',
 poolPressure:'KV 缓存池压力',poolPressureAria:'KV 缓存池压力说明',
  poolPressureTip:`缓存等待：正在等待 KV 缓存池腾出空间的请求数，大于 0 需立即关注；占用率不高时也可能出现：池内空间碎片化或等待槽位释放
 累计等待：请求为等待空间累计耗费的时间（秒），只增不减
冷重算次数：旧会话被移出 KV 缓存池后重新执行 prefill 的次数，持续增长说明池容量趋于不足
占位会话数：已离开但仍保留 KV 缓存占用的会话数，可随时复用
快照搬移次数：缓存快照被搬移的累计次数，与冷重算次数结合观察
碎片整理次数：KV 缓存池碎片整理（紧凑化）的累计次数，增长较快说明碎片较多
压缩预留次数：新请求预留空间被压缩的累计次数，增长说明池内空间趋于紧张
快照搬移/碎片整理/压缩预留均属 KV 缓存池空间回收机制，日常增长属正常调度行为`,
 missCache:'cache 数据缺失',cumWait:'累计等待',coldRecompute:'冷重算次数',heldSessions:'占位会话数',
 snapReloc:'快照搬移次数',defrag:'碎片整理次数',reservedCompress:'压缩预留次数',
 promptCache:'Prompt 缓存',promptCacheAria:'Prompt 缓存说明',
 promptCacheTip:'Prompt 缓存与内存全景中的「磁盘读缓存」不同：前者是 halogen 的会话前缀缓存（位于进程内存中），后者是 Linux 内核的磁盘读缓存（主要为模型查找表）',
 hitRate:'命中率',tokenHitRate:'Token 命中率',cacheSize:'缓存大小',cacheEntries:'缓存条目数',
 memPano:'内存全景',memPanoAria:'内存全景说明',
 memPanoTip:`六段按占用者划分，合计 = 总内存
磁盘读缓存：内核文件读缓存，可随时回收，不构成负载
主机内存「可用」长期保持 16–20 GiB，为引擎运行安全余量（MemAvailable 低于 16 GiB 时引擎拒绝启动），并非闲置浪费`,
 missMem:'内存数据缺失',dnSub:'= GTT 内存',
 panoEng:'推理引擎（模型+KV 缓存池）',panoProc:'其他进程',panoKern:'内核占用',
 panoRead:'磁盘读缓存（可回收）',panoFree:'空闲',panoPin:'其他驻留',
 guideSleep:'模型待命中，开始推理后自动恢复显示数据',
 guideOff:'引擎未响应 — 请检查 halogen 容器是否在运行，恢复后将自动显示数据',
 shortSleep:'待命中',shortOff:'等待引擎恢复',collecting:'采集中…',
 noSlotData:'暂无槽位数据，恢复后自动出现',slotN:'槽位 {i}',slotOffline:'离线',slotGenerating:'生成中',slotIdle:'空闲',
 engineRunning:'引擎运行中',engineNotResp:'引擎未响应',lastUpdated:'最后更新 {t}',
 healthErr:'异常',healthOk:'正常',
 waitTipKnown:'排队等待≥1 秒共 {n} 次 / 累计 {s} 秒；已放弃 = 客户端断连或排队超时',
 waitTipUnknown:'排队等待≥1 秒的次数与累计时间（秒）；已放弃 = 客户端断连或排队超时',
 dataMissing:'数据缺失',noQueue:'无排队',
 entTipKnown:'已被覆盖重写 {n} 次：条目达到上限是常态，旧会话按 LRU 策略让位，不是故障',
 entTipUnknown:'条目 = 当前缓存的会话前缀数 / 容量上限',
 footerMain:'采集间隔约 {iv} 秒 · 历史窗口约 {wm} 分钟 · 只读监控，不影响 halogen 运行',
 footerErr:' · 采集异常：{errs} 本轮数据缺失',listSep:'、',
 connRetry:'无法连接仪表盘服务器，正在重试…',secUnit:' 秒',
 tipEng:'模型权重约 60 GiB + KV 缓存池（GPU 映射）{gtt} GiB + 运行时堆',
 tipProc:'除推理引擎外全部进程的 RSS 合计',tipKern:'内核 Slab + 内核栈 KernelStack + 页表 PageTables',
 tipRead:'内核文件读缓存，可随时回收，不构成负载',tipFree:'MemAvailable 约 {mav} GiB（含可回收缓存）',
 tipPin:'未能归入前五段的内存（tmpfs/共享内存/驱动驻留等）'
},
'en':{
 title:'halogen Monitoring',brandSide:'halogen Monitoring',modelInfo:'Model Info',ctxLen:'Context Length',
 kvPrec:'KV Cache Precision',weightFmt:'Weight Format',engineVer:'Engine Version',slotCount:'Slot Count',
  slotState:'Slot State',slotStateAria:'Slot State Explanation',slotStateTip:'Speed Is Reported Only When Inference Completes; Per-Slot Data Not Provided. Display Starts at 1; Backend Numbering Starts at 0.',
   reqStatus:'Request Status',
    inFlight:'Active',queuedReq:'Queued Requests',queueWaits:'Queue Waits',timesUnit:'',abandoned:'Abandoned',
   inFlightAria:'Active Requests Explanation',
    inFlightTip:'Active = Requests the Engine Is Processing; Queued Requests = Requests Waiting for a Slot. Queue Waits = Times Waited 1 s or More; Abandoned = Client Disconnect or Queue Timeout.',
   colHealth:'Collection Health',colHealthAria:'Collection Health Explanation',
   colHealthTip:'Per-Channel (health/metrics/cache/props) Fetch Status for the Current Round: Green = OK, Red = Error (an Automatic Retry Already Ran Before Flagging). While a Channel Errors, Cards Keep Their Last Valid Values Instead of Blanking, and Update Automatically on Recovery.',
   probeLiveness:'Liveness Probe',probeMetrics:'Metrics',probeCache:'Cache Status',probeProps:'Engine Props',probeMeminfo:'Memory Info',
  tipHealth:`Data Feeds: Top-Bar Engine Status, Slot State, KV Cache Pool Usage
 (Slot Count / Active / Queued Requests / Total)`,
   tipMetrics:'Data Feeds: Four KPI Cards, KV Cache Pool Usage and Trend, Speculative Acceptance Rate, Sidebar Active/Queued Requests/Queue Waits/Abandoned Counts',
 tipCache:'Data Feeds: KV Cache Pool Pressure, Prompt Cache',
 tipProps:'Data Feeds: Sidebar Model Info (Alias/Slot Count), Sleeping State Detection',
  themeAria:'Switch Light/Dark Theme',themeDark:'Dark',themeLight:'Light',overviewH1:'Inference Monitoring Overview',
 connecting:'Connecting…',loading:'Loading…',pauseUpdates:'Pause Updates',resumeUpdates:'Resume Updates',
 langSwitchAria:'Switch Language',coreMetrics:'Core Metrics',
   decodeSpeed:'Decode Speed',decodeSpeedAria:'Decode Speed Explanation',peak30:'30m peak',
    decodeSpeedTip:`Main = Total Decode Speed (real-time estimate): In-Flight Requests × the most recently reported single-stream speed. The engine reports speed only when a request completes (it reports 0 while generating), so in-flight output is estimated at the latest reported single-stream speed; shows 0 when idle. Display is capped at the local bandwidth ceiling.
   Peak = highest total decode speed over the last 30 min.`,
  gpuUtil:'GPU Utilization',gpuTemp:'GPU Temperature',gpuPower:'GPU Power',gpuPowerAria:'GPU Power Explanation',
  gpuPowerTip:'Balanced Mode 85 W Is the Optimal Power Range for LLM Inference; 120 W Performance Mode Improves Inference Performance by Only ~2%.',
 kvUsage:'KV Cache Pool Usage',kvUsageAria:'KV Cache Pool Usage Explanation',
  kvUsageTip:`Usage = Share of Used Positions in the KV Cache Pool.
 100% Usage Does Not Mean Overload: with a Full Pool, Queued Requests 0 and Cold Recompute 0 Are Normal; by Design the Engine Allocates Memory to Saturation.
 Usage Drops via Two Paths: (1) Version Replacement (Old-Version Caches Replaced by New Versions, at Zero Cost) (2) LRU Eviction (Evicted Sessions Re-Run One Prefill on Their Next Request, Cold Recompute +1)
 Reserved = space claimed by in-flight requests (prompt + max_tokens, including the not-yet-generated part); independent of Used and not counted in it`,
  queuing:'Cache Waiting',modelSleeping:'Model Sleeping',missMetrics:'Metrics Data Unavailable',
  used:'Used',reserved:'Reserved',total:'Total',waitingData:'Waiting for Data…',
  reservedTip:'Reserved = space claimed by in-flight requests (prompt + max_tokens, including the not-yet-generated part); independent of Used and not counted in it',
 specAccept:'Speculative Acceptance Rate',totalReqs:'Total Requests',queueState:'Queue State',
 poolPressure:'KV Cache Pool Pressure',poolPressureAria:'KV Cache Pool Pressure Explanation',
  poolPressureTip:`Cache Waiting: Number of Requests Waiting for Space in the KV Cache Pool; above 0 Requires Immediate Attention; it May Also Appear when Usage Is Not High: Fragmented Pool Space or Slots Being Released
 Cumulative Wait: Total Time (Seconds) Requests Have Spent Waiting for Space; Only Increases
 Cold Recompute Count: Times Old Sessions Re-Ran Prefill after Being Evicted from the KV Cache Pool; Sustained Growth Indicates Pool Capacity Trending Short
 Held Session Count: Sessions That Have Left but Still Hold KV Cache Occupancy; Reusable Anytime
 Snapshot Relocation Count: Cumulative Times Cache Snapshots Were Relocated; Observe Together with Cold Recompute Count
 Defragment Count: Cumulative KV Cache Pool Defragmentation (Compaction) Runs; Fast Growth Indicates Fragmentation
 Reserved Compression Count: Cumulative Times New-Request Reserved Space Was Compressed; Growth Indicates Pool Space Tightening
 Snapshot Relocation/Defragment/Reserved Compression Are All KV Cache Pool Space-Reclaim Mechanisms; Daily Growth Is Normal Scheduling Behavior`,
 missCache:'Cache Data Unavailable',cumWait:'Cumulative Wait',coldRecompute:'Cold Recompute Count',heldSessions:'Held Session Count',
 snapReloc:'Snapshot Relocation Count',defrag:'Defragment Count',reservedCompress:'Reserved Compression Count',
 promptCache:'Prompt Cache',promptCacheAria:'Prompt Cache Explanation',
  promptCacheTip:'Prompt Cache Differs from "Disk Read Cache" in Memory Panorama: the Former Is halogen’s Session Prefix Cache (in Process Memory); the Latter Is the Linux Kernel’s Disk Read Cache (Mostly Model Lookup Tables)',
 hitRate:'Hit Rate',tokenHitRate:'Token Hit Rate',cacheSize:'Cache Size',cacheEntries:'Cache Entry Count',
 memPano:'Memory Panorama',memPanoAria:'Memory Panorama Explanation',
  memPanoTip:`Six Segments Split by Owner; Sum = Total Memory
 Disk Read Cache: Kernel File Read Cache, Reclaimable Anytime, Does Not Constitute Load
 Host "Available" Memory Stays at 16–20 GiB as the Engine Safety Margin (below 16 GiB MemAvailable the Engine Refuses to Start); Not Idle Waste`,
 missMem:'Memory Data Unavailable',dnSub:'= GTT Memory',
 panoEng:'Inference Engine (Model + KV Cache Pool)',panoProc:'Other Processes',panoKern:'Kernel Usage',
 panoRead:'Disk Read Cache (Reclaimable)',panoFree:'Free',panoPin:'Other Pinned',
 guideSleep:'Model Sleeping; Data Resumes When Inference Starts',
 guideOff:'Engine Not Responding — Check the halogen Container; Data Resumes Automatically',
 shortSleep:'Sleeping',shortOff:'Waiting for Engine Recovery',collecting:'Collecting…',
  noSlotData:'No Slot Data Yet; It Reappears Automatically after Recovery',slotN:'Slot {i}',slotOffline:'Offline',slotGenerating:'Generating',slotIdle:'Idle',
  engineRunning:'Engine Running',engineNotResp:'Engine Not Responding',lastUpdated:'Last Updated {t}',
  healthErr:'Error',healthOk:'OK',
  waitTipKnown:'Queue Waits ≥1 s: {n} Times / Cumulative {s} s; Abandoned = Client Disconnect or Queue Timeout',
  waitTipUnknown:'Count of Queue Waits ≥1 s and Cumulative Time (Seconds); Abandoned = Client Disconnect or Queue Timeout',
  dataMissing:'Data Unavailable',noQueue:'No Queue',
  entTipKnown:'Overwritten {n} Times: Entries at Capacity Is Normal; Old Sessions Are Evicted by LRU; Not a Fault',
 entTipUnknown:'Entries = Cached Session Prefixes / Capacity Limit',
 footerMain:'Sampling Interval ~{iv} s · History Window ~{wm} min · Read-Only Monitoring; Does Not Affect halogen',
 footerErr:' · Collection Error: {errs} Data Unavailable This Round',listSep:', ',
 connRetry:'Cannot Reach the Dashboard Server, Retrying…',secUnit:' s',
 tipEng:'Model Weights ~60 GiB + KV Cache Pool (GPU Mapped) {gtt} GiB + Runtime Heap',
  tipProc:'Total RSS of All Processes Except the Inference Engine',tipKern:'Kernel Slab + KernelStack + PageTables',
  tipRead:'Kernel File Read Cache; Reclaimable Anytime; Does Not Constitute Load',tipFree:'MemAvailable ~{mav} GiB (Including Reclaimable Cache)',
  tipPin:'Memory Not Fitting the First Five Segments (tmpfs/Shared Memory/Driver Pinned, Etc.)'
},
'ko':{
 title:'halogen 모니터링',brandSide:'halogen 모니터링',modelInfo:'모델 정보',ctxLen:'컨텍스트 길이',
 kvPrec:'KV 캐시 정밀도',weightFmt:'가중치 형식',engineVer:'엔진 버전',slotCount:'슬롯 수',
  slotState:'슬롯 상태',slotStateAria:'슬롯 상태 설명',slotStateTip:'속도는 추론 완료 시에만 보고되며, 슬롯별 데이터는 제공되지 않습니다. 표시는 1부터 시작하며, 백엔드 번호는 0부터 시작합니다.',
   reqStatus:'요청 상태',
    inFlight:'진행 중',queuedReq:'대기열 요청',queueWaits:'대기열 대기',timesUnit:'회',abandoned:'포기',
   inFlightAria:'진행 중 설명',
   inFlightTip:'진행 중 = 엔진이 처리 중인 요청 수; 대기열 요청 = 슬롯을 기다리는 요청 수. 대기열 대기 = 1초 이상 대기한 횟수; 포기 = 클라이언트 연결 끊김 또는 대기열 시간 초과.',
  colHealth:'수집 상태',colHealthAria:'수집 상태 설명',
  colHealthTip:'채널별(health/metrics/cache/props) 이번 라운드 수집 상태: 초록 = 정상, 빨강 = 오류(판정 전 자동 재시도 1회 완료). 채널 오류 중에도 각 카드는 마지막 유효 값을 유지하며 공란이 되지 않고, 복구 후 자동 갱신됩니다.',
   probeLiveness:'생존 탐지',probeMetrics:'지표 수집',probeCache:'캐시 상태',probeProps:'엔진 속성',probeMeminfo:'메모리 정보',
  tipHealth:`데이터 사용처: 상단바 엔진 상태, 슬롯 상태, KV 캐시 풀 사용률
 (슬롯 수 / 진행 중 / 대기열 요청 / 총계)`,
  tipMetrics:'데이터 사용처: KPI 4개 카드, KV 캐시 풀 사용률 및 추이, 투기적 허용률, 사이드바 진행 중/대기열 요청/대기열 대기/포기 집계',
 tipCache:'데이터 사용처: KV 캐시 풀 압력, Prompt 캐시',
 tipProps:'데이터 사용처: 사이드바 모델 정보(별칭/슬롯 수), 절전 상태 판정',
  themeAria:'라이트/다크 테마 전환',themeDark:'다크',themeLight:'라이트',overviewH1:'추론 모니터링 개요',
 connecting:'연결 중…',loading:'불러오는 중…',pauseUpdates:'갱신 일시정지',resumeUpdates:'갱신 재개',
 langSwitchAria:'언어 전환',coreMetrics:'핵심 지표',
   decodeSpeed:'디코딩 속도',decodeSpeedAria:'디코딩 속도 설명',peak30:'30분 최고',
    decodeSpeedTip:`주 값 = 총 디코딩 속도(실시간 추정): 진행 중인 요청 수 × 마지막으로 보고된 단일 스트림 속도. 엔진은 요청 완료 시에만 속도를 보고하므로(생성 중에는 0 보고) 진행 중인 산출물은 마지막 보고된 단일 스트림 속도로 추정되며, 유휴 시 0을 표시합니다. 표시는 이 머신의 대역폭 상한으로 제한됩니다.
   최고값 = 최근 30분 총 디코딩 속도 중 최고치.`,
 gpuUtil:'GPU 사용률',gpuTemp:'GPU 온도',gpuPower:'GPU 전력',gpuPowerAria:'GPU 전력 설명',
 gpuPowerTip:'균형 모드 85 W는 LLM 추론의 최적 전력 구간이며, 120 W 성능 모드는 추론 성능을 약 2%만 향상시킵니다.',
 kvUsage:'KV 캐시 풀 사용률',kvUsageAria:'KV 캐시 풀 사용률 설명',
 kvUsageTip:`사용률 = KV 캐시 풀의 사용 중인 위치 비율입니다.
사용률 100%는 과부하를 의미하지 않습니다: 풀이 가득 찬 상태에서 대기열 요청 0, 콜드 재계산 0은 정상이며, 엔진은 설계대로 메모리를 포화까지 할당합니다.
 사용률이 줄어드는 경로는 두 가지입니다: (1) 버전 교체 회수(구버전 캐시가 신버전으로 교체, 비용 없음) (2) LRU 퇴출(풀에서 제거된 세션은 다음 요청 시 prefill을 한 번 다시 실행, 콜드 재계산 +1)
 예약 = 진행 중인 요청이 선언한 공간 요구량(prompt + max_tokens, 아직 생성되지 않은 부분 포함). 사용량과 독립적이며 사용량에 포함되지 않음`,
  queuing:'캐시 대기',modelSleeping:'모델 절전 중',missMetrics:'metrics 데이터 없음',
  used:'사용',reserved:'예약',total:'총계',waitingData:'데이터 대기 중…',
  reservedTip:'예약 = 진행 중인 요청이 선언한 공간 요구량(prompt + max_tokens, 아직 생성되지 않은 부분 포함). 사용량과 독립적이며 사용량에 포함되지 않음',
 specAccept:'투기적 허용률',totalReqs:'총 요청 수',queueState:'대기열 상태',
 poolPressure:'KV 캐시 풀 압력',poolPressureAria:'KV 캐시 풀 압력 설명',
  poolPressureTip:`캐시 대기: KV 캐시 풀의 공간을 기다리는 요청 수, 0보다 크면 즉시 확인이 필요합니다. 사용률이 높지 않아도 나타날 수 있습니다: 풀 내 공간 단편화 또는 슬롯 해제 대기
 누적 대기: 공간 대기를 위해 소비한 누적 시간(초), 감소하지 않음
콜드 재계산 수: 구세션이 KV 캐시 풀에서 제거된 뒤 prefill을 다시 실행한 횟수, 지속 증가는 풀 용량 부족을 의미
점유 세션 수: 떠났지만 KV 캐시 점유를 유지하는 세션 수, 언제든 재사용 가능
스냅샷 이동 수: 캐시 스냅샷이 이동된 누적 횟수, 콜드 재계산 수와 함께 관찰
단편화 정리 수: KV 캐시 풀 단편화 정리(압축) 누적 횟수, 빠르게 증가하면 단편화가 많은 것
예약 압축 수: 새 요청 예약 공간이 압축된 누적 횟수, 증가는 풀 공간이 촉박해짐을 의미
스냅샷 이동/단편화 정리/예약 압축은 모두 KV 캐시 풀 공간 회수 메커니즘으로, 일상적 증가는 정상 스케줄링 동작입니다`,
 missCache:'cache 데이터 없음',cumWait:'누적 대기',coldRecompute:'콜드 재계산 수',heldSessions:'점유 세션 수',
 snapReloc:'스냅샷 이동 수',defrag:'단편화 정리 수',reservedCompress:'예약 압축 수',
 promptCache:'Prompt 캐시',promptCacheAria:'Prompt 캐시 설명',
 promptCacheTip:'Prompt 캐시는 메모리 파노라마의 "디스크 읽기 캐시"와 다릅니다: 전자는 halogen의 세션 접두어 캐시(프로세스 메모리 내)이며, 후자는 Linux 커널의 디스크 읽기 캐시(주로 모델 탐색 테이블)입니다',
 hitRate:'적중률',tokenHitRate:'Token 적중률',cacheSize:'캐시 크기',cacheEntries:'캐시 항목 수',
 memPano:'메모리 파노라마',memPanoAria:'메모리 파노라마 설명',
 memPanoTip:`6개 구간은 점유자 기준으로 구분하며, 합계 = 총 메모리
디스크 읽기 캐시: 커널 파일 읽기 캐시로 언제든 회수 가능하며 부하를 구성하지 않음
호스트 메모리 "가용"은 16–20 GiB를 유지하며 엔진 실행 안전 여유분입니다(MemAvailable이 16 GiB 미만이면 엔진은 시작을 거부함), 유휴 낭비가 아님`,
 missMem:'메모리 데이터 없음',dnSub:'= GTT 메모리',
 panoEng:'추론 엔진(모델+KV 캐시 풀)',panoProc:'기타 프로세스',panoKern:'커널 점유',
 panoRead:'디스크 읽기 캐시(회수 가능)',panoFree:'빈 영역',panoPin:'기타 상주',
 guideSleep:'모델 절전 중; 추론이 시작되면 데이터가 자동으로 복구됩니다',
 guideOff:'엔진 응답 없음 — halogen 컨테이너 실행 여부를 확인하세요. 복구 후 데이터가 자동으로 표시됩니다',
 shortSleep:'절전',shortOff:'엔진 복구 대기',collecting:'수집 중…',
 noSlotData:'슬롯 데이터 없음, 복구 후 자동으로 표시됩니다',slotN:'슬롯 {i}',slotOffline:'오프라인',slotGenerating:'생성 중',slotIdle:'유휴',
 engineRunning:'엔진 실행 중',engineNotResp:'엔진 응답 없음',lastUpdated:'마지막 갱신 {t}',
 healthErr:'오류',healthOk:'정상',
 waitTipKnown:'1초 이상 대기열 대기 {n}회 / 누적 {s}초; 포기 = 클라이언트 연결 끊김 또는 대기열 시간 초과',
 waitTipUnknown:'1초 이상 대기열 대기 횟수와 누적 시간(초); 포기 = 클라이언트 연결 끊김 또는 대기열 시간 초과',
 dataMissing:'데이터 없음',noQueue:'대기열 없음',
 entTipKnown:'{n}회 덮어쓰기됨: 항목 용량 도달은 정상이며, 구세션은 LRU 정책에 따라 밀려나고, 장애가 아닙니다',
 entTipUnknown:'항목 = 현재 캐시된 세션 접두어 수 / 용량 상한',
 footerMain:'수집 주기 약 {iv}초 · 히스토리 창 약 {wm}분 · 읽기 전용 모니터링, halogen 실행에 영향 없음',
 footerErr:' · 수집 오류: {errs} 이번 라운드 데이터 없음',listSep:', ',
 connRetry:'대시보드 서버에 연결할 수 없습니다, 재시도 중…',secUnit:'초',
 tipEng:'모델 가중치 약 60 GiB + KV 캐시 풀(GPU 매핑) {gtt} GiB + 런타임 힙',
 tipProc:'추론 엔진을 제외한 모든 프로세스의 RSS 합계',tipKern:'커널 Slab + KernelStack + PageTables',
 tipRead:'커널 파일 읽기 캐시, 언제든 회수 가능, 부하를 구성하지 않음',tipFree:'MemAvailable 약 {mav} GiB(회수 가능 캐시 포함)',
 tipPin:'앞의 다섯 구간에 포함되지 않은 메모리(tmpfs/공유 메모리/드라이버 상주 등)'
},
'de':{
 title:'halogen-Überwachung',brandSide:'halogen-Überwachung',modelInfo:'Modellinfo',ctxLen:'Kontextlänge',
 kvPrec:'KV-Cache-Präzision',weightFmt:'Gewichtsformat',engineVer:'Engine-Version',slotCount:'Slotanzahl',
  slotState:'Slotstatus',slotStateAria:'Erklärung zum Slotstatus',slotStateTip:'Die Geschwindigkeit wird nur bei Abschluss der Inferenz berichtet; pro Slot werden keine Daten bereitgestellt. Die Anzeige beginnt bei 1; die Backend-Nummerierung beginnt bei 0.',
   reqStatus:'Anfragestatus',
    inFlight:'Aktiv',queuedReq:'Anstehende Anfragen',queueWaits:'Wartefälle',timesUnit:'',abandoned:'Aufgegeben',
   inFlightAria:'Erklärung zu Aktiv',
   inFlightTip:'Aktiv = Anfragen, die die Engine gerade bearbeitet; Anstehende Anfragen = Anfragen, die auf einen Slot warten. Wartefälle = Male mit ≥1 s Warten; Aufgegeben = Client-Trennung oder Warteschlangen-Timeout.',
  colHealth:'Erfassungsstatus',colHealthAria:'Erklärung zum Erfassungsstatus',
  colHealthTip:'Kanalbezogener (health/metrics/cache/props) Erfassungsstatus der laufenden Runde: Grün = OK, Rot = Fehler (ein automatischer Wiederholungsversuch wurde vor der Meldung bereits ausgeführt). Während ein Kanal fehlerhaft ist, behalten die Karten ihre zuletzt gültigen Werte, statt leer zu werden, und aktualisieren sich nach der Wiederherstellung automatisch.',
   probeLiveness:'Lebendigkeitstest',probeMetrics:'Metriken',probeCache:'Cache-Status',probeProps:'Engine-Eigenschaften',probeMeminfo:'Speicherinfo',
  tipHealth:`Daten für: Engine-Status der Kopfzeile, Slotstatus, KV-Cache-Pool-Auslastung
 (Slotanzahl / Aktiv / Anstehende Anfragen / Gesamt)`,
  tipMetrics:'Daten für: vier KPI-Karten, KV-Cache-Pool-Auslastung und Verlauf, Spekulationsakzeptanzrate, Seitenleisten-Zähler für Aktiv/Anstehende Anfragen/Wartefälle/Aufgegeben',
 tipCache:'Daten für: KV-Cache-Pool-Druck, Prompt-Cache',
 tipProps:'Daten für: Modellinfo der Seitenleiste (Alias/Slotanzahl), Schlafzustand-Erkennung',
  themeAria:'Helles/dunkles Theme wechseln',themeDark:'Dunkel',themeLight:'Hell',overviewH1:'Inferenz-Überwachungsübersicht',
 connecting:'Verbindung…',loading:'Lädt…',pauseUpdates:'Aktualisierung pausieren',resumeUpdates:'Aktualisierung fortsetzen',
 langSwitchAria:'Sprache wechseln',coreMetrics:'Kernmetriken',
   decodeSpeed:'Dekodiergeschwindigkeit',decodeSpeedAria:'Erklärung zur Dekodiergeschwindigkeit',peak30:'30m Spitze',
    decodeSpeedTip:`Hauptwert = Gesamte Dekodiergeschwindigkeit (Echtzeit-Schätzung): Laufende Anfragen × zuletzt berichtete Einzelstrom-Geschwindigkeit. Die Engine berichtet die Geschwindigkeit nur bei Abschluss einer Anfrage (während der Generierung wird 0 berichtet); laufende Ausgaben werden daher mit der zuletzt berichteten Einzelstrom-Geschwindigkeit geschätzt; im Leerlauf 0. Die Anzeige ist durch die lokale Bandbreiten-Obergrenze begrenzt.
   Spitze = höchste Gesamtdekodiergeschwindigkeit der letzten 30 min.`,
 gpuUtil:'GPU-Auslastung',gpuTemp:'GPU-Temperatur',gpuPower:'GPU-Leistung',gpuPowerAria:'Erklärung zur GPU-Leistung',
 gpuPowerTip:'Der Ausgewogenheitsmodus 85 W ist der optimale Leistungsbereich für LLM-Inferenz; der Leistungsmodus 120 W verbessert die Inferenzleistung nur um ~2%.',
 kvUsage:'KV-Cache-Pool-Auslastung',kvUsageAria:'Erklärung zur KV-Cache-Pool-Auslastung',
 kvUsageTip:`Auslastung = Anteil der belegten Positionen im KV-Cache-Pool.
100% Auslastung bedeutet keine Überlastung: Bei vollem Pool sind 0 wartende Anfragen und 0 Kalt-Neuberechnungen normal; die Engine belegt den Speicher bestimmungsgemäß bis zur Sättigung.
 Die Auslastung sinkt auf zwei Wegen: (1) Versionseratz (Cache alter Versionen wird durch neue ersetzt, kostenlos) (2) LRU-Verdrängung (verdrängte Sitzungen führen beim nächsten Request einmal Prefill erneut aus, Kalt-Neuberechnung +1)
 Reserviert = von laufenden Anfragen deklarierter Platzbedarf (Prompt + max_tokens, einschließlich noch nicht erzeugter Anteile); unabhängig von Belegt und nicht in Belegt enthalten`,
  queuing:'Cache-Warten',modelSleeping:'Modell im Schlafmodus',missMetrics:'metrics-Daten nicht verfügbar',
  used:'Belegt',reserved:'Reserviert',total:'Gesamt',waitingData:'Warte auf Daten…',
  reservedTip:'Reserviert = von laufenden Anfragen deklarierter Platzbedarf (Prompt + max_tokens, einschließlich noch nicht erzeugter Anteile); unabhängig von Belegt und nicht in Belegt enthalten',
 specAccept:'Spekulationsakzeptanzrate',totalReqs:'Anfragen gesamt',queueState:'Warteschlangenstatus',
 poolPressure:'KV-Cache-Pool-Druck',poolPressureAria:'Erklärung zum KV-Cache-Pool-Druck',
  poolPressureTip:`Cache-Warten: Anzahl der Anfragen, die auf Platz im KV-Cache-Pool warten; über 0 sofort beachten; kann auch bei niedriger Auslastung auftreten: fragmentierter Pool-Platz oder ausstehende Slot-Freigabe
 Kumulative Wartezeit: Gesamtzeit (Sekunden), die Anfragen auf Platz gewartet haben; steigt nur
Kalt-Neuberechnungen: Male, die alte Sitzungen nach der Verdrängung aus dem KV-Cache-Pool Prefill erneut ausführten; anhaltendes Wachstum deutet auf knappe Pool-Kapazität hin
Haltende Sitzungen: Sitzungen, die gegangen sind, aber KV-Cache-Belegung behalten; jederzeit wiederverwendbar
Snapshot-Umzüge: Kumulative Male, die Cache-Snapshots verschoben wurden; zusammen mit Kalt-Neuberechnungen beobachten
Fragmentierungs-Bereinigungen: Kumulative Male der KV-Cache-Pool-Defragmentierung (Komprimierung); schnelles Wachstum deutet auf Fragmentierung hin
Reservierungs-Komprimierungen: Kumulative Male, in denen reservierter Platz neuer Anfragen komprimiert wurde; Wachstum deutet auf knappen Pool-Platz hin
Snapshot-Umzüge/Fragmentierungs-Bereinigungen/Reservierungs-Komprimierungen sind Mechanismen zur Platzrückgewinnung des KV-Cache-Pools; alltägliches Wachstum ist normales Scheduling`,
 missCache:'cache-Daten nicht verfügbar',cumWait:'Kumulative Wartezeit',coldRecompute:'Kalt-Neuberechnungen',heldSessions:'Haltende Sitzungen',
 snapReloc:'Snapshot-Umzüge',defrag:'Fragmentierungs-Bereinigungen',reservedCompress:'Reservierungs-Komprimierungen',
 promptCache:'Prompt-Cache',promptCacheAria:'Erklärung zum Prompt-Cache',
 promptCacheTip:'Der Prompt-Cache unterscheidet sich vom "Disk-Lese-Cache" im Speicher-Panorama: Ersterer ist halogens Sitzungs-Präfix-Cache (im Prozessspeicher), Letzterer der Disk-Lese-Cache des Linux-Kernels (hauptsächlich Modell-Lookup-Tabellen)',
 hitRate:'Trefferquote',tokenHitRate:'Token-Trefferquote',cacheSize:'Cache-Größe',cacheEntries:'Cache-Einträge',
 memPano:'Speicher-Panorama',memPanoAria:'Erklärung zum Speicher-Panorama',
 memPanoTip:`Sechs Segmente nach Besitzer; Summe = Gesamtspeicher
Disk-Lese-Cache: Kernel-Datei-Lese-Cache, jederzeit rückforderbar, stellt keine Last dar
Der "verfügbare" Hauptspeicher bleibt dauerhaft bei 16–20 GiB als Sicherheitsmarge für die Engine (unter 16 GiB MemAvailable verweigert die Engine den Start); keine brachliegende Verschwendung`,
 missMem:'Speicherdaten nicht verfügbar',dnSub:'= GTT-Speicher',
 panoEng:'Inferenz-Engine (Modell + KV-Cache-Pool)',panoProc:'Andere Prozesse',panoKern:'Kernel-Belegung',
 panoRead:'Disk-Lese-Cache (rückforderbar)',panoFree:'Frei',panoPin:'Sonstiges Pinned',
 guideSleep:'Modell im Schlafmodus; Daten werden bei Inferenzbeginn automatisch wieder angezeigt',
 guideOff:'Engine antwortet nicht — Prüfen Sie den halogen-Container; Daten werden nach der Wiederherstellung automatisch angezeigt',
 shortSleep:'Schlafmodus',shortOff:'Warten auf Engine-Erholung',collecting:'Erfassung…',
 noSlotData:'Keine Slot-Daten; erscheinen nach der Wiederherstellung automatisch',slotN:'Slot {i}',slotOffline:'Offline',slotGenerating:'Generierung',slotIdle:'Leerlauf',
 engineRunning:'Engine läuft',engineNotResp:'Engine antwortet nicht',lastUpdated:'Zuletzt aktualisiert {t}',
 healthErr:'Fehler',healthOk:'OK',
 waitTipKnown:'Warten ≥1 s: {n} Mal / kumuliert {s} s; Aufgegeben = Client-Trennung oder Warteschlangen-Timeout',
 waitTipUnknown:'Anzahl der Wartefälle ≥1 s und kumulative Zeit (Sekunden); Aufgegeben = Client-Trennung oder Warteschlangen-Timeout',
 dataMissing:'Daten nicht verfügbar',noQueue:'Keine Warteschlange',
 entTipKnown:'{n} Mal überschrieben: Einträge am Limit ist normal; alte Sitzungen weichen nach LRU; kein Fehler',
 entTipUnknown:'Einträge = aktuell gecachte Sitzungs-Präfixe / Kapazitätsgrenze',
 footerMain:'Erfassungsintervall ~{iv} s · Historienfenster ~{wm} min · Nur-Lese-Überwachung; beeinflusst halogen nicht',
 footerErr:' · Erfassungsfehler: {errs} in dieser Runde nicht verfügbar',listSep:', ',
 connRetry:'Dashboard-Server nicht erreichbar, erneuter Versuch…',secUnit:' s',
 tipEng:'Modellgewichte ~60 GiB + KV-Cache-Pool (GPU-gemappt) {gtt} GiB + Runtime-Heap',
 tipProc:'RSS-Summe aller Prozesse außer der Inferenz-Engine',tipKern:'Kernel-Slab + KernelStack + PageTables',
 tipRead:'Kernel-Datei-Lese-Cache, jederzeit rückforderbar, stellt keine Last dar',tipFree:'MemAvailable ~{mav} GiB (einschließlich rückforderbarer Cache)',
 tipPin:'Speicher außerhalb der ersten fünf Segmente (tmpfs/freigegebener Speicher/Treiber-Pinned usw.)'
}
};
function t(k,p){var d=I18N[lang];var s=(k in d)?d[k]:((k in I18N['zh-CN'])?I18N['zh-CN'][k]:k);
  if(p)for(var key in p)s=s.split('{'+key+'}').join(String(p[key]));
  return s;}
function applyLang(){
  document.documentElement.lang=lang;
  try{localStorage.setItem('dash-lang',lang);}catch(e){}
  document.title=t('title');
  var ns=document.querySelectorAll('[data-i18n]'),i;
  for(i=0;i<ns.length;i++)ns[i].textContent=t(ns[i].getAttribute('data-i18n'));
  var ts=document.querySelectorAll('[data-i18n-tip]');
  for(i=0;i<ts.length;i++)ts[i].setAttribute('data-tip',t(ts[i].getAttribute('data-i18n-tip')));
  var as=document.querySelectorAll('[data-i18n-aria]');
  for(i=0;i<as.length;i++)as[i].setAttribute('aria-label',t(as[i].getAttribute('data-i18n-aria')));
  $('langTxt').textContent=LANG_NAMES[lang];
  var dark=document.documentElement.getAttribute('data-theme')==='dark';
  $('themeTxt').textContent=t(dark?'themeLight':'themeDark');
  var paused=document.body.classList.contains('paused');
  $('pauseTxt').textContent=paused?t('resumeUpdates'):t('pauseUpdates');
  $('pauseBtn').setAttribute('aria-label',paused?t('resumeUpdates'):t('pauseUpdates'));
  hideTip();tipEl.textContent='';
  if(lastData)render(lastData);
  else{$('status-txt').textContent=t('connecting');$('sub').textContent=t('loading');
    var hs=['hs-health','hs-metrics','hs-cache','hs-props'];
    for(i=0;i<hs.length;i++)$(hs[i]).textContent=t('collecting');
    $('kv-queue').textContent=t('collecting');
    $('sbe-kv-t').textContent=t('waitingData');$('sbe-pc-t').textContent=t('waitingData');}
}
/* ---------- 语言切换下拉菜单（w8：替代原点击循环） ---------- */
var langWrap=$('langWrap'),langMenu=$('langMenu');
function closeLangMenu(){langMenu.classList.remove('open');$('langBtn').setAttribute('aria-expanded','false');}
function openLangMenu(){
  langMenu.innerHTML='';
  for(var i=0;i<LANGS.length;i++){(function(code){
    var o=document.createElement('div');
    o.className='lang-opt';o.setAttribute('role','option');o.setAttribute('data-lang',code);o.tabIndex=0;
    o.setAttribute('aria-selected',code===lang?'true':'false');
    o.textContent=(code===lang?'✓ ':'')+LANG_NAMES[code];
    o.addEventListener('click',function(){if(code!==lang){lang=code;applyLang();}closeLangMenu();$('langBtn').focus();});
    langMenu.appendChild(o);})(LANGS[i]);}
  langMenu.classList.add('open');$('langBtn').setAttribute('aria-expanded','true');}
$('langBtn').addEventListener('click',function(e){
  e.stopPropagation();
  if(langMenu.classList.contains('open'))closeLangMenu();else openLangMenu();});
document.addEventListener('click',function(e){
  if(langMenu.classList.contains('open')&&!langWrap.contains(e.target))closeLangMenu();});
document.addEventListener('keydown',function(e){
  if(!langMenu.classList.contains('open'))return;
  if(e.key==='Escape'){closeLangMenu();$('langBtn').focus();return;}
  var opts=[].slice.call(langMenu.children),idx=opts.indexOf(document.activeElement);
  if(e.key==='ArrowDown'){e.preventDefault();(opts[idx+1]||opts[0]).focus();}
  else if(e.key==='ArrowUp'){e.preventDefault();(opts[idx-1]||opts[opts.length-1]).focus();}
  else if(e.key==='Enter'&&idx>=0){e.preventDefault();opts[idx].click();}});
/* ---------- 启动 ---------- */
var _st='light';try{_st=localStorage.getItem('halo-theme')||'light';}catch(e){}
applyTheme(_st);
applyLang();
gaugeInkCenter();
if(DEMO){render({});}
else{seed();connect();}
</script>
</body>
</html>
"""


# S2a-P02r (P1-4): 连接治理——并发上限 + 每连接超时 + listen backlog，防 DoS/slowloris/慢客户端；阈值全可配置
MAX_CONNS = int(os.environ.get("DASH_MAX_CONNS", "64"))
CONN_TIMEOUT = float(os.environ.get("DASH_CONN_TIMEOUT", "30"))
_conns = 0
_conns_lock = threading.Lock()
ThreadingHTTPServer.request_queue_size = 128  # backlog 5→128（listen() 在 __init__ 内读取，故改类属性）


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = CONN_TIMEOUT  # S2a-P02r: socket 读写超时；SSE keepalive ≤15s 不受影响

    def setup(self):
        global _conns
        with _conns_lock:
            _conns += 1
            self._over_cap = _conns > MAX_CONNS
        super().setup()

    def handle(self):
        if self._over_cap:  # S2a-P02r: 超限 → 503 即关，不进请求循环、不占工作线程
            try:
                self.request_version = "HTTP/1.1"  # send_response 依赖（正常路径由 parse_request 设置）
                self.send_response(503)
                self.send_header("Connection", "close")
                self.end_headers()
            except OSError:
                pass
            self.close_connection = True
            return
        try:
            super().handle()
        except OSError:
            pass  # 客户端断开（SSE 长连接关闭等）静默，不刷 traceback

    def finish(self):
        try:
            super().finish()
        finally:
            global _conns
            with _conns_lock:
                _conns -= 1

    def _send(self, code, ctype, body, extra=()):
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")  # S2a-P04r (P2-1): 防 MIME 嗅探
            self.send_header("X-Frame-Options", "DENY")  # S2a-P04r: 防点击劫持
            self.send_header("Referrer-Policy", "no-referrer")  # S2a-P04r: 不外泄来源
            for k, v in extra:
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def version_string(self):  # S2a-P03r (P1-5): Server 头去版本指纹（原 BaseHTTP/0.6 Python/3.12.3）
        return "dash"

    def _stream(self):
        """SSE：每采集拍只推 latest（chunked，history 由客户端本地滚动，技术调研 §2.2），
        15s 无拍发注释帧保活。json.dumps 在锁外做（锁内只浅拷贝快照，P2-3）。"""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            last = -1
            while True:
                frame = None
                snap = None
                with _cond:
                    while _tick == last:
                        if not _cond.wait(15.0):
                            break
                    if _tick != last:
                        last = _tick
                        snap = dict(_latest)
                if snap is not None:
                    frame = "data: " + json.dumps({"latest": snap}, ensure_ascii=False) + "\n\n"
                if frame is None:
                    frame = ": keepalive\n\n"
                chunk = frame.encode("utf-8")
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _host_ok(self):  # S2a-P01r (P1-3): Host 头白名单校验；剥端口、转小写、兼容 IPv6 方括号
        h = (self.headers.get("Host") or "").strip().lower().rstrip(".")
        if h.startswith("["):  # IPv6 字面量 [::1]:8873
            h = h[1:].split("]", 1)[0]
        elif h.count(":") == 1:  # host:port
            h = h.split(":", 1)[0]
        return h in ALLOWED_HOSTS

    def do_GET(self):
        if not self._host_ok():  # S2a-P01r (P1-3): Host 不在白名单 → 421（DNS rebinding 拒止）
            self._send(421, "text/plain; charset=utf-8", "misdirected host".encode())
            return
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", HTML.encode("utf-8"))
        elif path == "/data.json":
            with _lock:
                payload = {
                    "latest": dict(_latest),
                    "history": list(_history),
                    "interval": INTERVAL,
                    "window_min": round(HISTORY_LEN * INTERVAL / 60),
                    "decode_cap": DECODE_TPS_CAP,  # w15-合规P1-2：顶层下发封顶值供前端 SPD_CAP 对齐（history 行键契约不动）
                }
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            extra = ()
            if "gzip" in (self.headers.get("Accept-Encoding") or ""):
                body = gzip.compress(body, 6)
                extra = (("Content-Encoding", "gzip"),)
            self._send(200, "application/json; charset=utf-8", body, extra)
        elif path == "/stream":
            self._stream()
        else:
            self._send(404, "text/plain; charset=utf-8", "not found".encode())

    def log_message(self, *args):
        pass

    def log_request(self, code="-", size="-"):  # S2a-P06r (P2-2): 访问日志单行；不记 query（token 永不落日志）
        path = getattr(self, "path", "-").split("?")[0]
        print("%s %s %s %s %s" % (time.strftime("%Y-%m-%dT%H:%M:%S"),
                                  self.client_address[0], code, size, path), flush=True)


def main():
    # S2a-P05r (P0-2): 安全默认=只绑回环（陌生人下载后仅自己可访问）；开放范围用 DASH_BINDS 配置，不改码
    binds = [h.strip() for h in os.environ.get("DASH_BINDS", "127.0.0.1").split(",") if h.strip()]
    if not binds:  # w15-bugP1-3：配置笔误（如 DASH_BINDS=","）→ 友好报错，⛔ 裸 IndexError traceback
        raise SystemExit("DASH_BINDS is empty")
    if _WILDCARD_BIND:  # w15-bugP1-1：通配绑定下自动 Host 放行失效，引导显式配置白名单
        print("warning: DASH_BINDS binds a wildcard address (0.0.0.0/::) — auto Host allowlist is skipped; "
              "set DASH_ALLOWED_HOSTS (comma-separated hostnames/IPs) so real Host headers pass the 421 check", flush=True)
    threading.Thread(target=collector, daemon=True).start()
    threading.Thread(target=slow_collector, daemon=True).start()
    servers = None
    for port in PORTS:
        tried = []
        try:
            for host in binds:
                tried.append(ThreadingHTTPServer((host, port), Handler))
        except OSError:
            for s in tried:
                s.server_close()
            continue
        servers = tried
        if port != PORTS[0]:
            print("warning: port %d busy, fell back to %d" % (PORTS[0], port), flush=True)
        print("halogen-dash listening on " + ", ".join("http://%s:%d" % (h, port) for h in binds), flush=True)
        break
    if servers is None:
        raise SystemExit("no free port in %s for binds %s (interface down? check DASH_BINDS)" % (PORTS, binds))
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
    for t in threads:
        t.start()
    threads[0].join()


if __name__ == "__main__":
    main()
