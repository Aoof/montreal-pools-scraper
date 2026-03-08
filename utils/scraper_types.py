import time

class _TYPES(dict):
    def __init__(self):
        super().__init__()
        self['PISI'] = "Indoor swimming pool"
        self['PIEX'] = "Outdoor swimming pool"
        self['PATA'] = "Wading pool"
        self['JEUD'] = "Play fountains"

    def __getitem__(self, key):
        if isinstance(key, int):
            # Allow indexing by position
            keys = list(self.keys())
            return keys[key]
        return super().__getitem__(key)

    def __iter__(self):
        return iter(self.keys())

TYPES = _TYPES()

class PoolType:
    def __init__(self, name : str = TYPES['PISI'], description : str = ""):
        self.name = name
        self.description = description
    
    def __str__(self):
        for t in TYPES:
            if self.name == TYPES[t]:
                return t
        return self.name

class Pool:
    def __init__(self, 
                 name : str, 
                 url  : str,
                 geo_location: str,
                 type : PoolType = PoolType(),
                 address: str = "",
                 primary_image_url: str = "",
                 map_link: str = "",
                 phone: str = "",
                 createdAt: float = time.time(),
                 is_active: bool = True,
                 schedules: list = []):
        self.name = name
        self.type = type
        self.url = url
        self.address = address
        self.primary_image_url = primary_image_url
        self.map_link = map_link
        self.geo_location = geo_location
        self.phone = phone
        self.createdAt = createdAt
        self.is_active = is_active
        self.schedules = schedules

class Schedule:
    def __init__(self, day : str, start : float, end : float, createdAt : float = time.time()):
        self.day = day
        self.start = start
        self.end = end
        self.createdAt = createdAt