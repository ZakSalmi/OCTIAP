from typing import List, Dict

def get_indicators(pulses: List[Dict], pulse_type: str) -> List[Dict]:
    """
    Return a list of pulses of a specific type
    """
    indicators = (
        indicator
        for pulse in pulses
        if 'indicators' in pulse
        for indicator in pulse['indicators']
        if 'type' in indicator and indicator['type'] == pulse_type
    )
    return list(indicators)
