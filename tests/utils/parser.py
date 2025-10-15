def parse_log_line(line):
    """
    Parse a log line and return a dictionary with structured data.
    Expected log format:
    | action: <action> | result: <result> | client_id: <client_id> | message
    """
    parts = line.split('|')
    log_data = {}
    
    for part in parts:
        part = part.strip()
        if ': ' in part:
            key, value = part.split(': ', 1)
            log_data[key.strip()] = value.strip()
        else:
            log_data['message'] = part.strip()
    
    return log_data