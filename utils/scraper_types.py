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
                 pool_type : PoolType = PoolType(),
                 address: str = "",
                 primary_image_url: str = "",
                 phone: str = "",
                 is_active: bool = True,
                 schedules: list = [],
                 createdAt: float = time.time()):
        self.name = name
        self.pool_type = pool_type
        self.url = url
        self.address = address
        self.primary_image_url = primary_image_url
        geo_array = geo_location.split(":")
        if (geo_array != 2): raise Exception("Bad geographic data provided") 
        self.map_link = f"https://www.openstreetmap.org/?lat={geo_array[0]}&lon={geo_array[1]}&zoom=15"
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