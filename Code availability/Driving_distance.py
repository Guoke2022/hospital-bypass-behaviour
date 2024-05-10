import osmnx as ox
import requests
import math
from haversine import haversine, Unit

def f_haversine(point1, point2):
    point1 = (point1[1], point1[0])
    point2 = (point2[1], point2[0])
    distance = haversine(point1, point2, unit=Unit.METERS)

    return distance

def get_road_distances(start, end, graph, cpus, CITY):
    try:
        start_lng, start_lat = start
        end_lng, end_lat = end
        start = GCJ022WGS84(start_lng, start_lat)
        end = GCJ022WGS84(end_lng, end_lat)

        # find the nearest node to the start/end location
        from_node = ox.nearest_nodes(graph, start[0], start[1])
        to_node = ox.nearest_nodes(graph, end[0], end[1])

        # the shortest route(dijkstra)
        route = ox.shortest_path(graph, from_node, to_node, weight="length", cpus=cpus)
        if route is None:
            raise ValueError("No path found between the given nodes")

        edges = ox.utils_graph.route_to_gdf(graph, route, weight="length")

        # Check if edges DataFrame is empty (no edges found in the route)
        if edges.empty:
            raise ValueError("graph contains no edges")

        length = edges['length'].sum()


    except ValueError as ve:
        if "graph contains no edges" in str(ve):
            length = f_haversine(start, end)
            print("graph contains no edges")
        elif "No path found between the given nodes" in str(ve):
            length = f_haversine(start, end)
            print("No path found between the given nodes")
        else:
            length = f_haversine(start, end)


    return length


a = 6378245.0
ee = 6.693421622965943e-3
def GCJ022WGS84(lng, lat):
    if ((not lng or not lat) or not (lng >= -180 and lng <= 180 and lat >= -90 and lat <= 90)):
        print('Invalid lnglat in GCJ022WGS84', lng, lat)
        return
    if (not (lng > 73.66 and lng < 135.05 and lat > 3.86 and lat < 53.55)):
        print('Outof China', lng, lat)
        return

    dlat = transLat(lng - 105.0, lat - 35.0)
    dlng = transLng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * math.pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * math.pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * math.pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [lng * 2 - mglng, lat * 2 - mglat]

def transLat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 * math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * math.pi) + 40.0 * math.sin(lat / 3.0 * math.pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * math.pi) + 320 * math.sin(lat * math.pi / 30.0)) * 2.0 / 3.0
    return ret

def transLng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 * math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * math.pi) + 40.0 * math.sin(lng / 3.0 * math.pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * math.pi) + 300.0 * math.sin(lng / 30.0 * math.pi)) * 2.0 / 3.0
    return ret