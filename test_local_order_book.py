#!/usr/bin/env python3
"""
Binance Futures 本地订单簿测试脚本。
参考：
  - binance-futures-connector-python (UMFutures + UMFuturesWebsocketClient)
  - binance-toolbox-python manage_local_order_book.py (算法参考)
  - NexusTrade Rust 订单簿引擎 (nexus-book / nexus-binance)

Binance 官方深度维护算法：
  1. 订阅 WS `{symbol}@depth`，开始缓存增量事件
  2. GET REST `/fapi/v1/depth?symbol=?&limit=1000` 获取快照
  3. 丢弃缓存中 `u < lastUpdateId` 的过期事件
  4. 将快照写入本地簿
  5. 从第一个 `U <= lastUpdateId+1 && u >= lastUpdateId+1` 的事件开始应用
  6. 每个新事件的 `pu` 应等于上一事件的 `u`，否则 gap → 从步骤 2 重建
  7. 数量为绝对值，qty=0 表示删除该档位

用法:
  python3 test_local_order_book.py [SYMBOL] [--testnet]
"""

import sys
import json
import time
import threading
import logging
from collections import deque
from typing import Dict, List, Optional, Tuple

from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

# ── 日志配置 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("local_book")


# ═══════════════════════════════════════════════════════════════════
# 订单簿数据结构
# ═══════════════════════════════════════════════════════════════════

class OrderBook:
    """本地订单簿。bids 降序，asks 升序，均为 [(price, qty), ...]"""

    def __init__(self):
        self.last_update_id: int = 0
        self.bids: List[Tuple[float, float]] = []  # 降序
        self.asks: List[Tuple[float, float]] = []  # 升序
        self.ready: bool = False
        self.snapshot_id: int = 0
        self.update_count: int = 0

    def apply_snapshot(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], last_update_id: int):
        """写入 REST 快照，替换整个簿。"""
        self.bids = sorted(bids, key=lambda x: x[0], reverse=True)
        self.asks = sorted(asks, key=lambda x: x[0])
        self.snapshot_id = last_update_id
        self.last_update_id = last_update_id
        self.ready = True
        self.update_count = 0

    def apply_delta(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], final_update_id: int):
        """应用增量更新。qty=0 删除，否则 upsert。"""
        if not self.ready:
            return
        for price, qty in bids:
            self._upsert_side(self.bids, price, qty, is_bid=True)
        for price, qty in asks:
            self._upsert_side(self.asks, price, qty, is_bid=False)
        self.last_update_id = final_update_id
        self.update_count += 1

    def _upsert_side(self, side: List[Tuple[float, float]], price: float, qty: float, is_bid: bool):
        """插入或更新单侧档位。qty=0 删除。"""
        if qty <= 0:
            # 删除
            for i, (p, _) in enumerate(side):
                if p == price:
                    side.pop(i)
                    return
        else:
            # 二分查找插入位置
            lo, hi = 0, len(side)
            while lo < hi:
                mid = (lo + hi) // 2
                if is_bid:
                    if side[mid][0] > price:
                        lo = mid + 1
                    elif side[mid][0] < price:
                        hi = mid
                    else:
                        # 找到精确匹配，更新
                        side[mid] = (price, qty)
                        return
                else:
                    if side[mid][0] < price:
                        lo = mid + 1
                    elif side[mid][0] > price:
                        hi = mid
                    else:
                        side[mid] = (price, qty)
                        return
            # 未找到，插入
            side.insert(lo, (price, qty))

    def best_bid(self) -> Optional[Tuple[float, float]]:
        if self.bids:
            return self.bids[0]
        return None

    def best_ask(self) -> Optional[Tuple[float, float]]:
        if self.asks:
            return self.asks[0]
        return None

    def top_of_book(self) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba:
            return (bb, ba)
        return None

    def depth(self, levels: int = 5):
        return {
            "bids": self.bids[:levels],
            "asks": self.asks[:levels],
            "last_update_id": self.last_update_id,
            "update_count": self.update_count,
        }

    def clear(self):
        """清空簿，失活。"""
        self.ready = False
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.update_count = 0


# ═══════════════════════════════════════════════════════════════════
# CachedDelta — WS 事件缓存
# ═══════════════════════════════════════════════════════════════════

class CachedDelta:
    __slots__ = ("first_update_id", "final_update_id", "prev_final_id",
                 "bids", "asks", "event_time")

    def __init__(self, first_update_id: int, final_update_id: int,
                 prev_final_id: int, bids: List[Tuple[float, float]],
                 asks: List[Tuple[float, float]], event_time: int = 0):
        self.first_update_id = first_update_id
        self.final_update_id = final_update_id
        self.prev_final_id = prev_final_id
        self.bids = bids
        self.asks = asks
        self.event_time = event_time


# ═══════════════════════════════════════════════════════════════════
# 主控制器：WebSocket + REST + 订单簿维护
# ═══════════════════════════════════════════════════════════════════

class BinanceLocalBook:
    """Binance Futures 本地订单簿。"""

    def __init__(self, symbol: str = "BTCUSDT", testnet: bool = False):
        self.symbol = symbol.upper()
        self.testnet = testnet

        if testnet:
            self.rest_url = "https://testnet.binancefuture.com"
            self.ws_url = "wss://stream.binancefuture.com/ws"
        else:
            self.rest_url = "https://fapi.binance.com"
            self.ws_url = "wss://fstream.binance.com/ws"

        self.book = OrderBook()
        self.buffer: deque[CachedDelta] = deque()
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

        # REST 客户端
        self.client = UMFutures(base_url=self.rest_url)

        # WS 客户端（在独立线程中运行）
        self.ws_client: Optional[UMFuturesWebsocketClient] = None
        self.ws_thread: Optional[threading.Thread] = None

    # ── REST 快照 ──

    def fetch_snapshot(self) -> dict:
        """获取 REST 深度快照（limit=1000）。"""
        return self.client.depth(self.symbol, limit=1000)

    # ── WS 消息处理 ──

    def on_message(self, _conn, message: str):
        """WebSocket 回调：收到消息时触发。"""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        # 只处理 depthUpdate 事件
        if data.get("e") != "depthUpdate":
            return

        bids_raw = data.get("b", [])
        asks_raw = data.get("a", [])
        first_update_id = data.get("U", 0)
        final_update_id = data.get("u", 0)
        prev_final_id = data.get("pu", 0)

        bids = [(float(p), float(q)) for p, q in bids_raw]
        asks = [(float(p), float(q)) for p, q in asks_raw]

        delta = CachedDelta(
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            prev_final_id=prev_final_id,
            bids=bids,
            asks=asks,
            event_time=data.get("E", 0),
        )

        with self.lock:
            # 步骤 3：如果簿已就绪，丢弃过期事件
            if self.book.ready and final_update_id <= self.book.last_update_id:
                return

            self.buffer.append(delta)

    # ── 簿维护循环 ──

    def _maintain_loop(self):
        """后台线程：Binance 官方深度算法。"""
        last_u: int = 0           # 上一个成功应用的 final_update_id
        need_bridge: bool = True  # 快照后是否在等待第一个桥接事件
        retry_count: int = 0

        while not self._stop_event.is_set():
            # ── 阶段 1：拉 REST 快照 ──
            # 首轮等 WS 预热；重试时不等待，立即拉
            if retry_count == 0:
                time.sleep(0.3)
            else:
                time.sleep(0.05)

            try:
                snap = self.fetch_snapshot()
            except Exception as e:
                logger.error(f"REST snapshot failed: {e}")
                time.sleep(1.0)
                continue

            snapshot_id = snap["lastUpdateId"]
            snap_bids = [(float(p), float(q)) for p, q in snap["bids"]]
            snap_asks = [(float(p), float(q)) for p, q in snap["asks"]]

            with self.lock:
                # 步骤 3：丢弃 u < snapshot_id+1 的过期事件
                self.buffer = deque(
                    d for d in self.buffer
                    if d.final_update_id >= snapshot_id + 1
                )

                # 步骤 4：写入快照
                self.book.apply_snapshot(snap_bids, snap_asks, snapshot_id)
                last_u = snapshot_id

                # 步骤 5：在 buffer 中找桥接事件
                apply_idx = 0
                for i, d in enumerate(self.buffer):
                    if d.first_update_id <= last_u + 1 and d.final_update_id >= last_u + 1:
                        self.book.apply_delta(d.bids, d.asks, d.final_update_id)
                        last_u = d.final_update_id
                        apply_idx = i + 1
                        break

                for _ in range(apply_idx):
                    if self.buffer:
                        self.buffer.popleft()

                need_bridge = (apply_idx == 0)

            if need_bridge:
                retry_count += 1
                if retry_count <= 5:
                    continue  # 立即重拉快照（无长等待）
                # 超过 5 次仍无桥接 → 强制推进：取第一个 U > last_u 的事件
                logger.warning(f"No bridge after {retry_count} retries, forcing sync...")
                with self.lock:
                    if self.buffer:
                        d = self.buffer.popleft()
                        self.book.apply_delta(d.bids, d.asks, d.final_update_id)
                        last_u = d.final_update_id
                        need_bridge = False
                    else:
                        # buffer 完全空 → 等 WS 积累
                        time.sleep(0.5)
                        continue

            retry_count = 0
            logger.info(
                f"Book ready: snapshot_id={snapshot_id}, "
                f"best_bid={self.book.best_bid()}, "
                f"best_ask={self.book.best_ask()}, "
                f"buf={len(self.buffer)}"
            )

            # ── 阶段 2：持续消费增量 ──
            while not self._stop_event.is_set():
                delta = None
                with self.lock:
                    if not self.buffer:
                        delta = None
                    else:
                        d = self.buffer[0]

                        if d.final_update_id <= last_u:
                            self.buffer.popleft()
                            continue

                        if d.prev_final_id != last_u:
                            logger.warning(
                                f"Gap: pu={d.prev_final_id} != last_u={last_u}. "
                                "Re-syncing..."
                            )
                            last_u = 0
                            self.book.clear()
                            need_bridge = True
                            self.buffer.popleft()  # 丢弃引发 gap 的事件
                            break

                        self.buffer.popleft()
                        delta = d

                if delta is None:
                    time.sleep(0.01)
                    continue

                self.book.apply_delta(delta.bids, delta.asks, delta.final_update_id)
                last_u = delta.final_update_id

    # ── 启动 / 停止 ──

    def start(self):
        """启动订单簿同步。"""
        logger.info(f"Starting local order book for {self.symbol} "
                     f"({'TESTNET' if self.testnet else 'MAINNET'})")

        # 后台维护线程
        maintain_thread = threading.Thread(
            target=self._maintain_loop,
            daemon=True,
            name="book-maintain",
        )
        maintain_thread.start()

        # WebSocket 线程
        def ws_runner():
            self.ws_client = UMFuturesWebsocketClient(
                on_message=self.on_message,
                is_combined=False,
            )
            # 订阅 depth diff 流（100ms 更新速度）
            self.ws_client.diff_book_depth(
                symbol=self.symbol.lower(),
                speed=100,
                id=1,
            )

        self.ws_thread = threading.Thread(
            target=ws_runner,
            daemon=True,
            name="ws-reader",
        )
        self.ws_thread.start()

        return maintain_thread

    def stop(self):
        """停止同步。"""
        self._stop_event.set()
        if self.ws_client:
            self.ws_client.stop()

    # ── 只读查询 ──

    def get_top(self) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        with self.lock:
            return self.book.top_of_book()

    def get_depth(self, levels: int = 5) -> dict:
        with self.lock:
            return self.book.depth(levels)

    def get_stats(self) -> dict:
        with self.lock:
            return {
                "ready": self.book.ready,
                "snapshot_id": self.book.snapshot_id,
                "last_update_id": self.book.last_update_id,
                "update_count": self.book.update_count,
                "bids_levels": len(self.book.bids),
                "asks_levels": len(self.book.asks),
                "buffer_size": len(self.buffer),
            }


# ═══════════════════════════════════════════════════════════════════
# 打印辅助
# ═══════════════════════════════════════════════════════════════════

def format_level(price: float, qty: float) -> str:
    return f"{price:>12.4f}  x {qty:<10.4f}"


# ═══════════════════════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════════════════════

def main():
    symbol = "BTCUSDT"
    testnet = False
    duration = 60  # 默认运行 60 秒

    # 简易参数解析
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--testnet":
            testnet = True
        elif args[i].startswith("--duration="):
            duration = int(args[i].split("=")[1])
        elif not args[i].startswith("--"):
            symbol = args[i].upper()
        i += 1

    print(f"{'='*60}")
    print(f"  Binance Futures Local Order Book")
    print(f"  Symbol:  {symbol}")
    print(f"  Network: {'TESTNET' if testnet else 'MAINNET'}")
    print(f"  Runtime: {duration}s")
    print(f"{'='*60}")

    # 创建并启动
    runner = BinanceLocalBook(symbol=symbol, testnet=testnet)
    maint = runner.start()

    # 等待簿就绪
    print("\nWaiting for order book snapshot...")
    for _ in range(30):
        stats = runner.get_stats()
        if stats["ready"]:
            print(f"  Book ready! snapshot_id={stats['snapshot_id']}\n")
            break
        time.sleep(0.5)
    else:
        print("  Timeout waiting for snapshot. Exiting.")
        runner.stop()
        return

    # 主循环：每秒打印 Best Bid/Ask
    start_ts = time.time()
    try:
        while time.time() - start_ts < duration:
            top = runner.get_top()
            stats = runner.get_stats()

            if top:
                (bid_p, bid_q), (ask_p, ask_q) = top
                spread = ask_p - bid_p
                spread_pct = (spread / bid_p) * 100 if bid_p > 0 else 0

                ts_str = time.strftime("%H:%M:%S")
                print(
                    f"[{ts_str}] "
                    f"Bid: {bid_p:>10.2f} x {bid_q:<8.4f} | "
                    f"Ask: {ask_p:>10.2f} x {ask_q:<8.4f} | "
                    f"Spread: {spread:.2f} ({spread_pct:.4f}%) | "
                    f"Updates: {stats['update_count']:>6d} | "
                    f"Buffer: {stats['buffer_size']}"
                )
            else:
                ts_str = time.strftime("%H:%M:%S")
                print(f"[{ts_str}] Book not ready, buffer={stats['buffer_size']}")

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    # 打印最终 5 档深度
    depth = runner.get_depth(5)
    print(f"\n{'─'*50}")
    print("  Final Depth (5 levels):")
    print(f"  {'Bids':>20}  {'Asks':>20}")
    print(f"  {'─'*20}  {'─'*20}")
    for i in range(5):
        bid_str = format_level(*depth["bids"][i]) if i < len(depth["bids"]) else " " * 25
        ask_str = format_level(*depth["asks"][i]) if i < len(depth["asks"]) else " " * 25
        print(f"  {bid_str}  {ask_str}")
    print(f"{'─'*50}")

    # 清理
    runner.stop()
    print(f"\nTotal updates applied: {runner.get_stats()['update_count']}")
    print("Done.")


if __name__ == "__main__":
    main()
