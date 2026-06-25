def load_properties(path):
    props = {}

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                pass
            else:
                key, value = line.split("=")
                props[key] = value
    return props
