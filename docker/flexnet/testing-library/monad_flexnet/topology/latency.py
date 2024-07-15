from dataclasses import dataclass

@dataclass
class LatencyProfile:
    ms: int = 0
    jitter: int = 0
    correlation: float = 0.0
    loss: float = 0.0
