def to_int(value: str) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def to_float(value: str) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)
