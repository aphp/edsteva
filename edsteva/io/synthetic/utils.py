def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            if key[0] != "P":
                yield key
            yield from recursive_items(value)
        else:
            yield key
