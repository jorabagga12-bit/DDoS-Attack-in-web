"""
Pure Python Load Test — k6 ka asyncio equivalent
1 lakh concurrent users tak support karta hai
"""

import asyncio
import aiohttp
import time
import random
import signal
import sys
from dataclasses import dataclass, field
from typing import List
from collections import defaultdict


# ============================================
# CONFIG
# ============================================
TARGET_URL = "https://target-website.com/"
REQUEST_TIMEOUT = 30       # seconds
# ============================================


# ============================================
# STAGES (k6 ke stages[] ka Python version)
# ============================================
STAGES = [
    # (duration_sec, target_users)
    (30,   1000),
    (60,   1000),
    (60,   5000),
    (60,   5000),
    (60,   10000),
    (60,   10000),
    (120,  25000),
    (60,   25000),
    (120,  50000),
    (60,   50000),
    (180,  100000),    # 🔥 PEAK
    (600,  100000),    # 🔥 10 min torture
    (60,   50000),
    (60,   10000),
    (60,   0),
]


# ============================================
# METRICS TRACKER
# ============================================
@dataclass
class Metrics:
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    status_5xx: int = 0
    timeouts: int = 0
    connection_errors: int = 0
    response_times: List[float] = field(default_factory=list)
    status_codes: dict = field(default_factory=lambda: defaultdict(int))
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record(self, status: int, duration: float, error: str = None):
        async with self.lock:
            self.total_requests += 1
            self.response_times.append(duration)
            self.status_codes[status] += 1

            if error == "timeout":
                self.timeouts += 1
                self.failed += 1
            elif error == "connection":
                self.connection_errors += 1
                self.failed += 1
            elif status == 0 or status >= 500:
                self.failed += 1
                if status >= 500:
                    self.status_5xx += 1
            else:
                self.successful += 1

    def percentile(self, p: float) -> float:
        if not self.response_times:
            return 0.0
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * p / 100)
        return sorted_times[min(idx, len(sorted_times) - 1)]

    def print_summary(self):
        print("\n" + "=" * 60)
        print("📊 FINAL REPORT")
        print("=" * 60)
        print(f"Total Requests     : {self.total_requests:,}")
        print(f"✅ Successful      : {self.successful:,}")
        print(f"❌ Failed          : {self.failed:,}")
        print(f"   ├─ 5xx Errors   : {self.status_5xx:,}")
        print(f"   ├─ Timeouts     : {self.timeouts:,}")
        print(f"   └─ Conn Errors  : {self.connection_errors:,}")

        if self.total_requests:
            error_rate = self.failed / self.total_requests * 100
            print(f"\nError Rate         : {error_rate:.2f}%")

        if self.response_times:
            print(f"\n⏱️  Response Times:")
            print(f"   Avg             : {sum(self.response_times)/len(self.response_times)*1000:.0f} ms")
            print(f"   P50             : {self.percentile(50)*1000:.0f} ms")
            print(f"   P95             : {self.percentile(95)*1000:.0f} ms")
            print(f"   P99             : {self.percentile(99)*1000:.0f} ms")
            print(f"   Max             : {max(self.response_times)*1000:.0f} ms")

        print(f"\n📋 Status Codes:")
        for code, count in sorted(self.status_codes.items()):
            print(f"   {code:>5} : {count:,}")

        print("=" * 60)


# ============================================
# WORKER (har virtual user)
# ============================================
class VirtualUser:
    def __init__(self, session: aiohttp.ClientSession, metrics: Metrics, user_id: int):
        self.session = session
        self.metrics = metrics
        self.user_id = user_id
        self.running = True

    async def run(self):
        while self.running:
            start = time.perf_counter()
            status = 0
            error = None

            try:
                async with self.session.get(
                    TARGET_URL,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    await resp.read()
                    status = resp.status

            except asyncio.TimeoutError:
                error = "timeout"
            except aiohttp.ClientConnectionError:
                error = "connection"
            except Exception as e:
                error = f"other: {type(e).__name__}"

            duration = time.perf_counter() - start
            await self.metrics.record(status, duration, error)

            # k6 ka sleep(Math.random() * 2 + 1)
            await asyncio.sleep(random.uniform(1, 3))

    def stop(self):
        self.running = False


# ============================================
# RAMP CONTROLLER
# ============================================
class RampController:
    def __init__(self, stages, metrics):
        self.stages = stages
        self.metrics = metrics
        self.active_users: List[VirtualUser] = []
        self.tasks: List[asyncio.Task] = []
        self.stop_event = asyncio.Event()

    def current_target(self, elapsed: float) -> int:
        cumulative = 0
        prev = 0
        for duration, users in self.stages:
            cumulative += duration
            if elapsed < cumulative:
                time_in = elapsed - (cumulative - duration)
                progress = time_in / duration
                return int(prev + (users - prev) * progress)
            prev = users
        return 0

    async def spawn(self, session: aiohttp.ClientSession, user_id: int):
        user = VirtualUser(session, self.metrics, user_id)
        self.active_users.append(user)
        task = asyncio.create_task(user.run())
        self.tasks.append(task)

    async def stop_one(self):
        if self.active_users:
            user = self.active_users.pop()
            user.stop()

    async def monitor(self):
        start = time.time()
        total_duration = sum(d for d, _ in self.stages)
        user_counter = 0

        connector = aiohttp.TCPConnector(
            limit=0,              # unlimited
            limit_per_host=0,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            last_log = 0

            while True:
                elapsed = time.time() - start
                if elapsed > total_duration:
                    break

                target = self.current_target(elapsed)
                current = len(self.active_users)

                # Spawn new users
                while len(self.active_users) < target:
                    user_counter += 1
                    await self.spawn(session, user_counter)
                    if len(self.active_users) % 500 == 0:
                        await asyncio.sleep(0)  # yield

                # Remove extra users
                while len(self.active_users) > target:
                    await self.stop_one()

                # Log every 5 seconds
                if elapsed - last_log >= 5:
                    last_log = elapsed
                    rps = self.metrics.total_requests / max(elapsed, 1)
                    err_rate = (
                        self.metrics.failed / self.metrics.total_requests * 100
                        if self.metrics.total_requests else 0
                    )
                    print(
                        f"[{elapsed:6.0f}s] "
                        f"Users: {len(self.active_users):>6,} | "
                        f"Req: {self.metrics.total_requests:>8,} | "
                        f"RPS: {rps:>7.0f} | "
                        f"Errors: {err_rate:>5.2f}%"
                    )

                await asyncio.sleep(0.5)

            # Stop all
            for user in self.active_users:
                user.stop()
            self.active_users.clear()

            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)


# ============================================
# MAIN
# ============================================
async def main():
    print("=" * 60)
    print("🔥 PYTHON STRESS TEST")
    print("=" * 60)
    print(f"Target    : {TARGET_URL}")
    print(f"Peak Users: {max(u for _, u in STAGES):,}")
    print(f"Duration  : {sum(d for d, _ in STAGES)} sec")
    print("=" * 60)
    print("⚠️  Only run this against systems you OWN or have WRITTEN PERMISSION for.")
    print("=" * 60 + "\n")

    metrics = Metrics()
    controller = RampController(STAGES, metrics)

    loop = asyncio.get_running_loop()

    def shutdown():
        print("\n\n🛑 Stopping test...")
        controller.stop_event.set()
        for user in controller.active_users:
            user.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass  # Windows

    try:
        await controller.monitor()
    except asyncio.CancelledError:
        pass

    metrics.print_summary()


if __name__ == "__main__":
    # Windows ke liye Proactor loop disable (aiohttp compatible)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")