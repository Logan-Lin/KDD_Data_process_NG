def per_delta(start, end, delta):
    curr = start
    while curr <= end:
        yield curr
        curr += delta
