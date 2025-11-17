from pyspark.sql import functions as F

def calc_distance(lat1, lon1, lat2, lon2):
    r = 6371
    return 2 * r * F.asin(F.sqrt(
        F.pow(F.sin((F.radians(lat2) - F.radians(lat1)) / 2), 2) +
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) *
        F.pow(F.sin((F.radians(lon2) - F.radians(lon1)) / 2), 2)
    ))
