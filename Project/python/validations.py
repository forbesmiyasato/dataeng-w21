import datetime

test = {"EVENT_NO_TRIP": "168733674", "EVENT_NO_STOP": "168733676", "OPD_DATE": "24-SEP-20", 
"VEHICLE_ID": "4036", "METERS": "75843", "ACT_TIME": "34217", "VELOCITY": "5", "DIRECTION": "80", 
"RADIO_QUALITY": "", "GPS_LONGITUDE": "-122.661302", "GPS_LATITUDE": "45.630505", "GPS_SATELLITES": "9", 
"GPS_HDOP": "0.8", "SCHEDULE_DEVIATION": "99"}

def validateVelocity(data):
    key = 'VELOCITY'
    if key not in data:
        print("Velocity doesn't exist in data!")
        return False
    try:
        velocity = int(data[key])
    except ValueError:
        print( "The record has an abnormal velocity of {}".format(velocity))
        return False

    if (0 <= velocity <= 50) is False:
        print( "The record has an abnormal velocity of {}".format(velocity))
    return True

def validateLatitude(data):
    key = 'GPS_LATITUDE'
    if key not in data:
        print("Latitude doesn't exist in data!")
        return False

    try:
        latitude = float(data[key])
    except ValueError:
        print("The record has an abnormal latitude of {}".format(latitude))
        return False

    latitudeOfPortland = 45.5051
    errorRange = 10

    if (latitudeOfPortland - errorRange < latitude < latitudeOfPortland + errorRange) is False:
        print("The record has an abnormal latitude of {}".format(latitude))
        return False
    return True

def validateLongitude(data):
    key = 'GPS_LONGITUDE'
    if key not in data:
        print("Longitude doesn't exist in data!")
        return False

    try:
        longitude = float(data[key])
    except ValueError:
        print("The record has an abnormal longitude of {}".format(longitude))
        return False

    longitudeOfPortland = -122.6750
    errorRange = 10
    if (longitudeOfPortland - errorRange < longitude < longitudeOfPortland + errorRange) is False:
        print("The record has an abnormal longitude of {}".format(longitude))
        return False
    return True

def validateOperationDay(data):
    key = "OPD_DATE"
    if key not in data:
        print("Operation day doesn't exist in data!")
        return

    date = data[key].split("-")
    try:
        day = int(date[0])
        year = int(date[2])
    except ValueError:
        print("The record has an invalid date")
        return False
    months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
    if date[1] not in months:
        print("The record has an invalid month")
        return False

    month = months[date[1]]
    try:
        newDate = datetime.datetime(year, month, day)
    except ValueError:
        print("The record has an invalid date: day - {}, month - {}, year - {}".format(day, month, year))
        return False
    return True

def validateDirection(data):
    key = "DIRECTION"
    if key not in data:
        print("DIRECTION doesn't exist in data!")
        return False
    try:
        direction = int(data[key])
    except ValueError:
        print("The record has an abnormal direction of {}!".format(direction))
        return False
    if (0 <= direction <= 360) is False:
        print("The record has an abnormal direction of {}!".format(direction))
        return False
    return True

def validateMeters(data):
    key = "METERS"
    if key not in data:
        print("METERS doesn't exist in data!")
        return False

    meters = data[key]
    if meters.isnumeric() is False:
        print("The record has an abornal meters of {}!".format(meters))
        return False
    return True

def validateRadioQuality(data):
    key = "RADIO_QUALITY"
    if key not in data:
        print("Radio quality doesn't exist in data!")
        return False

    radio_quality = data[key]
    if radio_quality.strip() != "":
        print("The record has an abornal radio quality of {}!".format(radio_quality))
        return False
    return True

def validateHDOP(data):
    key = "GPS_HDOP"
    if key not in data:
        print("HDOP doesn't exist in data!")
        return False
    try:
        hdop = float(data[key])
    except ValueError:
        print("The record has a poor HDOP value of {}!".format(hdop))
        return False
    if hdop >= 20:
        print("The record has a poor HDOP value of {}!".format(hdop))
        return False
    return True

def validateSatellites(data):
    key = "GPS_SATELLITES"
    if key not in data:
        print("Satellites doesn't exist in data!")
        return False

    try:
        satellites = int(data[key])
    except ValueError:
        print("The record has an abnormal amount of satellites of {}!".format(satellites))
        return False
    if (0 < satellites < 50) is False:
        print("The record has an abnormal amount of satellites of {}!".format(satellites))
        return False
    return True

def validateScheduleDeviation(data):
    key = "SCHEDULE_DEVIATION"
    if key not in data:
        print("Schedule Deviation doesn't exist in data!")
        return False

    sd = data[key]
    if sd.isnumeric() is False:
        print("The record has an abnormal schedule deviation of {}!".format(sd))
        return False
    return True

def validateActTime(data):
    key = "ACT_TIME"
    if key not in data:
        print("Actual time doesn't exist in data!")
        return False

    try:
        act_time = int(data[key])
    except ValueError:
        print("The record has an abnormal actual time of {}!".format(act_time))
        return False

    if 0 < act_time < 86400 is False:
        print("The record has an abnormal actual time of {}!".format(act_time))
        return False
    return True

def validateVehicleID(data):
    key = "VEHICLE_ID"
    if key not in data:
        print("Vehicle ID doesn't exist in data!")
        return False
    try:
        id = int(data[key])
    except ValueError:
        print("The record has an abnormal vehicle ID of {}!".format(id))
        return False
    return id 

def validateTripID(data):
    key = "EVENT_NO_TRIP"
    if key not in data:
        print("Trip ID doesn't exist in data!")
        return False
    try:
        id = int(data[key])
    except ValueError:
        print("The record has an abnormal trip ID of {}!".format(id))
        return False
    return id

def validateStopID(data):
    key = "EVENT_NO_STOP"
    if key not in data:
        print("Stop ID doesn't exist in data!")
        return False
    try:
        id = int(data[key])
    except ValueError:
        print("The record has an abnormal stop ID of {}!".format(id))
        return False
    return id

validateStopID(test)
validateTripID(test)
validateVehicleID(test)
validateActTime(test)
validateScheduleDeviation(test)
validateSatellites(test) 
validateHDOP(test)
validateRadioQuality(test)
validateMeters(test)
validateDirection(test)
validateVelocity(test)
validateLatitude(test)
validateLongitude(test)
validateOperationDay(test)
