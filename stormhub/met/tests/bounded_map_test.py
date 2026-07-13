"""Tests for the sliding-window executor helper ``bounded_map``."""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from stormhub.met.storm_catalog import bounded_map


class BoundedMapTest(unittest.TestCase):
    def test_processes_every_item_exactly_once(self):
        results = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            n = bounded_map(
                lambda x: ex.submit(lambda v=x: v * 2),
                range(50),
                results.append,
                max_in_flight=4,
            )
        self.assertEqual(n, 50)
        self.assertEqual(sorted(results), [i * 2 for i in range(50)])

    def test_window_never_exceeds_max_in_flight(self):
        max_in_flight = 3
        lock = threading.Lock()
        state = {"submitted": 0, "done": 0, "peak": 0}

        def work(v):
            time.sleep(0.002)  # let the window genuinely fill
            return v

        with ThreadPoolExecutor(max_workers=max_in_flight) as ex:

            def submit(x):
                with lock:
                    state["submitted"] += 1
                    live = state["submitted"] - state["done"]
                    state["peak"] = max(state["peak"], live)
                return ex.submit(work, x)

            def on_result(_r):
                with lock:
                    state["done"] += 1

            bounded_map(submit, range(100), on_result, max_in_flight=max_in_flight)

        # The window bounds outstanding (submitted-but-unharvested) work to the cap,
        # and with enough items it should actually reach the cap.
        self.assertLessEqual(state["peak"], max_in_flight)
        self.assertEqual(state["peak"], max_in_flight)

    def test_errors_are_routed_and_still_counted(self):
        errors = []
        results = []

        def work(x):
            if x % 2 == 0:
                raise ValueError(x)
            return x

        with ThreadPoolExecutor(max_workers=2) as ex:
            n = bounded_map(
                lambda x: ex.submit(work, x),
                range(10),
                results.append,
                on_error=errors.append,
                max_in_flight=2,
            )
        self.assertEqual(n, 10)
        self.assertEqual(len(errors), 5)
        self.assertEqual(sorted(results), [1, 3, 5, 7, 9])

    def test_exceptions_swallowed_without_handler(self):
        results = []

        def work(x):
            if x % 2 == 0:
                raise ValueError(x)
            return x

        with ThreadPoolExecutor(max_workers=2) as ex:
            n = bounded_map(lambda x: ex.submit(work, x), range(6), results.append, max_in_flight=2)
        self.assertEqual(n, 6)
        self.assertEqual(sorted(results), [1, 3, 5])

    def test_empty_workload(self):
        with ThreadPoolExecutor(max_workers=2) as ex:
            n = bounded_map(lambda x: ex.submit(lambda: x), [], lambda _r: None, max_in_flight=4)
        self.assertEqual(n, 0)

    def test_handles_various_sizes(self):
        for total, max_in_flight in [(1, 4), (4, 4), (7, 3), (200, 8)]:
            with ThreadPoolExecutor(max_workers=min(max_in_flight, 4)) as ex:
                results = []
                n = bounded_map(
                    lambda x: ex.submit(lambda v=x: v),
                    range(total),
                    results.append,
                    max_in_flight=max_in_flight,
                )
            self.assertEqual(n, total)
            self.assertEqual(sorted(results), list(range(total)))


if __name__ == "__main__":
    unittest.main()
