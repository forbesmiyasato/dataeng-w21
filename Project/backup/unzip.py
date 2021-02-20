import gzip
import  sys

zip_file = sys.argv[1]
out_file = sys.argv[2]

with gzip.open(zip_file, "rb") as data, open(out_file, "w") as output:
    output.write(data.read().decode("utf-8"))

print(zip_file, out_file)
