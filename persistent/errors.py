class UniquenessError(ValueError):
    def __init__(self, index_name):
        ValueError.__init__(self, index_name)


class NotFoundError(KeyError):
    pass
