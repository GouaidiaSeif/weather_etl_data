"""Alert event models."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class AlertEvent:
    """A single alert to evaluate, dedupe, and dispatch."""

    alert_key: str
    channel: str  # immediate | digest | ops
    title: str
    message: str
    severity: str = "warning"  # info | warning | critical
    city: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
