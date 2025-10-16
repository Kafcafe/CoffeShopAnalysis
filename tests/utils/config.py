yaml_template = (
    'server:\n'
    '  ip: "{ip}"\n'
    '  port: "{port}"\n'
    'log:\n'
    '  level: "{level}"\n'
    'conection:\n'
    '  limit: {limit}\n'
)

def set_server_config(ip="server", port="12345", level="INFO", limit=2):
    with open("./src/clientHandler/config.yaml", "w") as f:
        f.write(yaml_template.format(ip=ip, port=port, level=level, limit=limit))