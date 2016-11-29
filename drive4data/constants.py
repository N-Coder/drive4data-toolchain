import collections

Car = collections.namedtuple('Car', ['make', 'model', 'hybrid'])

chevrolet_volt = Car("Chevrolet", "Volt", True)
toyota_prius = Car("Toyota", "Prius Plug-in", True)
nissan_leaf = Car("Nissan", "Leaf", False)
smart_42ed = Car("Smart", "For Two Electric Drive", False)
ford_focus = Car("Ford", "Focus EV", False)

CARS = {
    1: chevrolet_volt,
    2: chevrolet_volt,
    3: nissan_leaf,
    4: toyota_prius,
    5: chevrolet_volt,
    6: smart_42ed,
    7: ford_focus,
    8: smart_42ed,
    9: smart_42ed,
    10: smart_42ed,
    11: smart_42ed,
}
