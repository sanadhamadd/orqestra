"""An offline, deterministic provider double for benchmarks.

``SeparatorReader`` answers what a reader of grouped numbers would: it drops the
grouping separators (``,``, ``_`` and spaces) from the raw string and returns
what is left. Any other character stays in the reply, so a reply that is not a
canonical digit string is judged invalid by the verifier, as it would be for a
real provider. Each answer reports ``len(raw)`` in the unit ``tokens``: a
synthetic figure that says nothing about any real provider's usage or cost.

It is not ``MockProvider``, so the invoice handler treats its route as billable
and requires a recorder: in a benchmark every call reaches the usage ledger.
It answers at once and never touches a network; this module imports only the
provider contract (an import scan in the tests holds it to that).
"""

from orqestra.calls.provider import ProviderRequest, ProviderResponse, ProviderUsage

SEPARATORS = frozenset({",", "_", " "})
USAGE_UNIT = "tokens"


class SeparatorReader:
    """A ``Provider`` that reads a grouped number by removing its separators."""

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        value = "".join(char for char in request.raw if char not in SEPARATORS)
        usage = ProviderUsage(str(len(request.raw)), USAGE_UNIT)
        return ProviderResponse(value=value, usage=usage)
