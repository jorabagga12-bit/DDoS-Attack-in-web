"""
Python Load Test — k6 ka Locust equivalent
Target: 1,00,000 concurrent users tak ramp-up
"""

import random
from locust import HttpUser, task, between, LoadTestShape, events


# ============================================
# USER BEHAVIOR (k6 ke default function jaisa)
# ============================================
class WebsiteUser(HttpUser):
    """
    Har virtual user homepage hit karega,
    phir 1-3 sec rukega (real user jaisa).
    """
    wait_time = between(1, 3)   # k6 ka sleep(Math.random()*2+1) equivalent
    
    # Timeout 30 sec (k6 ke timeout: '30s' jaisa)
    network_timeout = 30.0
    
    @task
    def homepage(self):
        with self.client.get(
            "/",
            name="Homepage",
            catch_response=True,
            timeout=30
        ) as response:
            
            # k6 check() equivalent
            if response.status_code == 0:
                response.failure("HTTP request did not complete")
            elif response.status_code >= 500:
                response.failure(f"Server Error: {response.status_code}")
            else:
                response.success()


# ============================================
# LOAD SHAPE (k6 ke stages[] equivalent)
# ============================================
class StressTestShape(LoadTestShape):
    """
    Ye k6 ke 'stages' array ko exactly replicate karta hai.
    (duration, target_users)
    """
    stages = [
        # (duration_sec, target_users)
        (30,   1000),      # ramp up
        (60,   1000),      # hold
        (60,   5000),      # ramp up
        (60,   5000),      # hold
        (60,   10000),     # ramp up
        (60,   10000),     # hold
        (120,  25000),     # ramp up
        (60,   25000),     # hold
        (120,  50000),     # ramp up
        (60,   50000),     # hold
        (180,  100000),    # ramp up to PEAK
        (600,  100000),    # 🔥 10 min at peak
        (60,   50000),     # ramp down
        (60,   10000),     # ramp down
        (60,   0),         # end
    ]

    def tick(self):
        run_time = self.get_run_time()

        cumulative = 0
        previous_users = 0

        for duration, users in self.stages:
            cumulative += duration

            if run_time < cumulative:
                # Linear interpolation (smooth ramp)
                time_in_stage = run_time - (cumulative - duration)
                progress = time_in_stage / duration

                current_users = int(
                    previous_users + (users - previous_users) * progress
                )

                # spawn_rate = how fast new users are added
                spawn_rate = max(
                    1,
                    abs(users - previous_users) // max(duration, 1)
                )

                return (current_users, spawn_rate)

            previous_users = users

        return None  # Test khatam