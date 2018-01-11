class ObjectDict(dict):
    """
    支持字典的点号读取属性
    >>> d = ObjectDict(x=1)
    >>> d.x
    1
    """
    def __getattr__(self, item):
        return self.get(item)

    def __setattr__(self, key, value):
        self[key] = value
