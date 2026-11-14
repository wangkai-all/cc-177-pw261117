"""Pointwise delivery intake workbench.

Orders, order lines and payments arrive in independently delivered batches.
This package persists the raw batches, correlates out-of-order arrivals via a
waiting area, and produces consistent published results with full lineage.
"""

from .engine import IntakeEngine

__all__ = ["IntakeEngine"]
