from typing import List, Dict

def get_indicators(pulses: List[Dict], pulse_type: str) -> List[Dict]:
    """
    Return a list of pulses of a specific type
    """
    indicators = [indicator for pulse in pulses for indicator in pulse['indicators'] if indicator['type'] == pulse_type]
    return indicators