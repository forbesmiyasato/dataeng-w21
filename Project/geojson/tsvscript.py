import csv, json, sys, re
from geojson import Feature, FeatureCollection, Point
features = []


if len(sys.argv) < 2:
    print("missing file names")
    sys.exit()

file_name = sys.argv[1]
output_name = sys.argv[2]
with open(file_name, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:]:
        row = re.split(r'\t+', line)
        # Uncomment these lines
        print(len(row))
        if len(row) < 6:
            continue
        lat = row[1]
        long = row[2]
        speed = row[4]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lat, long))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(float(speed)))
                    }
                )
            )
        except ValueError as e:
            print(e)
            continue

print(len(features))
collection = FeatureCollection(features)
with open(output_name, "w") as f:
    f.write('%s' % collection)
