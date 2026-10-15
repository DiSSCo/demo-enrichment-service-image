"""Extract structured coordinate information from DiSSCo specimen records."""
import re

def extract_coordinates(specimen):
    if specimen.get("locality"):
        text = specimen.get("locality")[:2000]
        latitude = None
        longitude = None
        lat_hem = None
        lon_hem = None

        # TODO: Add additional parsing methods to sum up certain cases
        # Decimal degrees with no hemisphere or hemisphere at the end
        match = re.search(r'(-?\d{1,3}\.\d+)\s*[°]?\s*([NS]?)[\s,;/]+\s*(-?\d{1,3}\.\d+)\s*[°]?\s*([OEW]?)', text)
        if match:
            lat, lat_hem, lon, lon_hem = match.groups()
            latitude, longitude = parse_coordinates(lat, lat_hem, lon, lon_hem)
        # Decimal degrees with no hemisphere or hemisphere at the beginning
        elif (match := re.search(r'([NS])\s*(-?\d{1,3}\.\d+)[\s,;/]+\s*([OEW])\s*(-?\d{1,3}\.\d+)', text)):
            lat_hem, lat, lon_hem, lon = match.groups()
            latitude, longitude = parse_coordinates(lat, lat_hem, lon, lon_hem)
        # Degrees, minutes, seconds with hemisphere at the end
        elif (match := re.search(r'(\d{1,3})[°]\s*(\d{1,2})[\'’]\s*(\d{1,2}(?:\.\d+)?)["”]?\s*([NS])[\s,;/]+\s*(\d{1,3})[°]\s*(\d{1,2})[\'’]\s*(\d{1,2}(?:\.\d+)?)["”]?\s*([OEW])', text)):
            lat_deg, lat_min, lat_sec, lat_hem, lon_deg, lon_min, lon_sec, lon_hem = match.groups()
            lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
            lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
            latitude, longitude = parse_coordinates(lat, lat_hem, lon, lon_hem)
        # Degrees, minutes, seconds with hemisphere at the beginning
        elif (match := re.search(r'([NS])\s*(\d{1,3})[°]\s*(\d{1,2})[\'’]\s*(\d{1,2}(?:\.\d+)?)["”]?[\s,;/]+\s*([OEW])\s*(\d{1,3})[°]\s*(\d{1,2})[\'’]\s*(\d{1,2}(?:\.\d+)?)["”]?', text)):
            lat_hem, lat_deg, lat_min, lat_sec, lon_hem, lon_deg, lon_min, lon_sec = match.groups()
            lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
            lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
            latitude, longitude = parse_coordinates(lat, lat_hem, lon, lon_hem)
        # Degrees and decimal minutes with hemisphere at the end
        elif (match := re.search(r'(\d{1,3})[°]\s*(\d{1,2}\.\d+)[\'’]?\s*([NS])[\s,;/]+\s*(\d{1,3})[°]\s*(\d{1,2}\.\d+)[\'’]?\s*([OEW])', text)):
            lat_deg, lat_min, lat_hem, lon_deg, lon_min, lon_hem = match.groups()
            lat = float(lat_deg) + float(lat_min)/60
            lon = float(lon_deg) + float(lon_min)/60
            latitude, longitude = parse_coordinates(lat, lat_hem, lon, lon_hem)
        # Degrees and decimal minutes with hemisphere at the beginning
        elif (match := re.search(r'([NS])\s*(\d{1,3})[°]\s*(\d{1,2}\.\d+)[\'’]?[\s,;/]+\s*([OEW])\s*(\d{1,3})[°]\s*(\d{1,2}\.\d+)[\'’]?', text)):
            lat_hem, lat_deg, lat_min, lon_hem, lon_deg, lon_min = match.groups()
            lat = float(lat_deg) + float(lat_min)/60
            lon = float(lon_deg) + float(lon_min)/60
            latitude, longitude = parse_coordinates(lat, lat_hem, lon, lon_hem)
        if latitude is not None and specimen.get("latitude") is None:
            specimen["latitude"] = latitude
        if longitude is not None and specimen.get("longitude") is None:
            specimen["longitude"] = longitude 
    return specimen

def parse_coordinates(lat, lat_hem, lon, lon_hem):
    lat = float(lat)
    lon = float(lon)
    if lat_hem.upper() == 'S':
        lat = -lat
    if lon_hem.upper() == 'W':
        lon = -lon
    return lat, lon

