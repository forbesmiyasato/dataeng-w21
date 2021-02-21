def validateTripId(data):
    key = 'trip_id'
    if key not in data:
        print("Trip ID doesn't exist in data.")
        return False

    id = data[key]

    try:
        id = int(data[key])
    except ValueError:
        print("The record has an abnormal trip ID of {}!".format(id))
        return False

    return id

def validateServiceKey(data):
    key = 'service_key'
    if key not in data:
        print("Service key doesn't exist in data.")
        return False

    service_key = data[key]
    service_keys = {'W': 'Weekdays', 'S': 'Saturday'}

    if service_key not in service_keys:
        print("The record has an invalid service key of {}.".format(service_key))
        return False

    return service_keys[service_key]


def validateDirection(data):
    key = 'direction'
    if key not in data:
        print("Direction doesn't exist in data.")
        return False

    direction = data[key]
    directions = {'0': 'Out', '1': 'Back'}
    if direction not in directions:
        print("The record has an invalid direction of {}.".format(direction))
        return False

    return directions[direction]

def validateRouteId(data):
    key = 'route_number'
    if key not in data:
        print("Route Id doesn't exist in data.")
        return False

    id = data[key]

    try:
        id = int(data[key])
    except ValueError:
        print("The record has an abnormal route ID of {}!".format(id))
        return False

    return id
