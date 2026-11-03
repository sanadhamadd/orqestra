"""Synthetic invoice verification: strict normalization, deterministic checks, and an
optional provider step that may only interpret an ambiguous amount string.

The schema and rules are a synthetic demonstration contract (see ``model.py``).
The batch handler (``handler.py``) is not registered with ``orqestra worker run``.
"""
