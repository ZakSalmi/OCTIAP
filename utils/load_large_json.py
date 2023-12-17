import ijson
import json

def load_large_json(file_path):
    with open(file_path, 'r') as file:
        parser = ijson.parse(file)
        pulses = []
        current_key = None
        buffer = ""

        for prefix, event, value in parser:
            if event == 'start_array':
                pulses = []
            elif event == 'map_key':
                current_key = value
            elif current_key and event == 'string':
                buffer += value

            if buffer and event.endswith('_end'):
                try:
                    pulse = json.loads(buffer)
                    pulses.append(pulse)
                    buffer = ""
                except json.JSONDecodeError:
                    print("Error parsing JSON object:", buffer)

    return pulses
