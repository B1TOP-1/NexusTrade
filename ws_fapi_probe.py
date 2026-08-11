#!/usr/bin/env python3
"""
Binance USDⓈ-M Futures WebSocket API (ws-fapi) 下单验证脚本。

验证 AI 提供的 ws-fapi 信息是否属实：
1. 连接端点 wss://ws-fapi.binance.com/ws-fapi/v1 是否可连通
2. session.status / order.place 方法是否可用、签名格式
3. 实测（安全：post-only 远离盘口不成交）

关键点（官方文档）：
- 签名仅支持 Ed25519 密钥（不是 HMAC-SHA256）—— HMAC 是否可用需实测
- apiKey 参与签名（在 params 里）
- params 除 signature 外按 key 排序 → 签名 → 编码
- 本地被墙走代理：ws://127.0.0.1:7897

用法：
  python3 ws_fapi_probe.py                    # 只测连通 + session.status
  python3 ws_fapi_probe.py --place            # + order.place post-only 下单
  python3 ws_fapi_probe.py --no-proxy         # VPS 直连（无代理）
"""

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import uuid
import websocket

WS_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 7897


def load_env():
    for line in open(".env"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def sign_ed25519(params: dict, private_key_pem: str) -> str:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives import serialization
    items = sorted((k, str(v)) for k, v in params.items() if k != "signature")
    payload = "&".join(f"{k}={v}" for k, v in items)
    key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    return base64.b64encode(key.sign(payload.encode("ASCII"))).decode("ASCII")


def sign_hmac(params: dict, secret: str) -> str:
    items = sorted((k, str(v)) for k, v in params.items() if k != "signature")
    payload = "&".join(f"{k}={v}" for k, v in items)
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def ws_request(ws, method: str, params: dict, sign_fn) -> dict:
    req_params = dict(params)
    req_params["timestamp"] = int(time.time() * 1000)
    req_params["apiKey"] = os.environ.get("BINANCE_API_KEY", "")
    req_params["signature"] = sign_fn(req_params)
    payload = {"id": str(uuid.uuid4()), "method": method, "params": req_params}
    ws.send(json.dumps(payload))
    raw = ws.recv()
    return json.loads(raw)


def main():
    load_env()
    api_key = os.environ.get("BINANCE_API_KEY", "")
    secret = os.environ.get("BINANCE_API_SECRET", "")
    ed_key = os.environ.get("BINANCE_ED25519_KEY", "")

    if not api_key or not secret:
        print("⚠ 缺少 BINANCE_API_KEY / BINANCE_API_SECRET")
        return

    place = "--place" in sys.argv
    use_proxy = "--no-proxy" not in sys.argv
    sign_method = "Ed25519" if ed_key else "HMAC-SHA256"

    print(f"{'='*60}")
    print(f"  Binance ws-fapi 验证")
    print(f"  端点: {WS_URL}")
    print(f"  网络: {'代理' if use_proxy else '直连'}")
    print(f"  签名: {sign_method}")
    print(f"{'='*60}")

    kw = {}
    if use_proxy:
        kw["http_proxy_host"] = PROXY_HOST
        kw["http_proxy_port"] = PROXY_PORT

    try:
        ws = websocket.create_connection(WS_URL, timeout=15, **kw)
        print("\n[1] 连接成功 ✓")

        # 签名函数
        if ed_key:
            sign_fn = lambda p: sign_ed25519(p, ed_key)
        else:
            sign_fn = lambda p: sign_hmac(p, secret)

        # session.status 只读
        try:
            resp = ws_request(ws, "session.status", {}, sign_fn)
            print(f"[2] session.status → {json.dumps(resp)[:250]}")
        except Exception as e:
            print(f"[2] session.status 失败: {e}")

        # account.status 认证端点（只读，验证签名算法是否正确）
        try:
            resp = ws_request(ws, "account.status", {}, sign_fn)
            print(f"[2b] account.status → {json.dumps(resp)[:250]}")
        except Exception as e:
            print(f"[2b] account.status 失败: {e}")

        # order.place
        if place:
            params = {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTX",
                "quantity": "0.001",
                "price": "63600.0",
                "recvWindow": 5000,
            }
            try:
                resp = ws_request(ws, "order.place", params, sign_fn)
                print(f"[3] order.place → {json.dumps(resp)[:300]}")
                if resp.get("status") == 200:
                    oid = resp["result"].get("orderId")
                    if oid:
                        cresp = ws_request(
                            ws, "order.cancel",
                            {"symbol": "BTCUSDT", "orderId": oid},
                            sign_fn,
                        )
                        print(f"[4] order.cancel → {json.dumps(cresp)[:200]}")
            except Exception as e:
                print(f"[3] order.place 失败: {e}")
        else:
            print("[3] （只读模式。加 --place 验证 order.place）")

        ws.close()
    except Exception as e:
        print(f"✗ 连接失败: {e}")

    print("\n完成 ✓")


if __name__ == "__main__":
    main()
