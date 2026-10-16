"""Whole-credit accounting for new comparisons; the upstream budget stays unchanged."""

from .policies import Budget


class WholeCreditBudget(Budget):
    def try_retry(self):
        if self.retries + 1 > self.ratio * self.requests + self.floor:
            return False
        self.retries += 1
        return True
