if __name__ == '__main__':
    import ckydb

    keys = ["hey", "hi", "salut", "bonjour", "hola", "oi", "mulimuta"]
    values = ["English", "English", "French", "French", "Spanish", "Portuguese", "Runyoro"]
    db = ckydb.connect("db",
                       max_file_size_kb=(4 * 1024),
                       vacuum_interval_sec=(5 * 60),
                       should_sanitize=False)

    # setting the keys
    for k, v in zip(keys, values):
        db.set(k, v)

    for i, k in enumerate(keys):
        assert values[i] == db.get(k)

    # updating keys
    new_values = ["Jane", "John", "Jean", "Marie", "Santos", "Ronaldo", "Aliguma"]
    for k, v in zip(keys, new_values):
        db.set(k, v)

    for i, k in enumerate(keys):
        assert new_values[i] == db.get(k)

    # deleting the keys
    for k in keys[:2]:
        db.delete(k)

    for k, v in zip(keys[2:], new_values[2:]):
        assert v == db.get(k)

    errors = []

    for k in keys[:2]:
        try:
            v = db.get(k)
        except ckydb.exc.NotFoundError as exc:
            errors.append(exc)

    assert len(errors) == len(keys[:2])

    # clear the database
    errors.clear()
    db.clear()

    for k in keys:
        try:
            v = db.get(k)
        except ckydb.exc.NotFoundError as exc:
            errors.append(exc)

    assert len(errors) == len(keys)
