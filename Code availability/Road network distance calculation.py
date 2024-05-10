import pandas as pd
import numpy as np
from haversine import haversine, Unit
from pathlib2 import Path
from pypinyin import lazy_pinyin
from Driving_distance import get_road_distances
import osmnx as ox
import time
from functools import partial
from multiprocessing import Pool
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

# TODO:Calculate the road network distance from the patient's home to the hospital
def home_process_row(index, row, graph, CITY):
    lat_home = row['grid_lat_hou']
    lon_home = row['grid_lon_hou']
    lat_hosp = row['POINT_Y']
    lon_hosp = row['POINT_X']
    road_distance = get_road_distances((lon_home, lat_home), (lon_hosp, lat_hosp), graph, cpus=8, CITY=CITY)
    print(CITY, index, road_distance)

    return road_distance

def main(CITY):
    PATH_RESULT2 = Path(f"...")
    data = pd.read_csv(PATH_RESULT2.joinpath(f'{CITY}剔除人数异常少的医院 加房价汇网格.csv'), encoding='utf-8')
    data0 = data[['POINT_X', 'POINT_Y', 'grid_lon_hou', 'grid_lat_hou']]
    graph = ox.load_graphml(
        filepath=f".../{CITY}.graphml")

    data0 = data0.drop_duplicates(subset=['POINT_X', 'POINT_Y', 'grid_lon_hou', 'grid_lat_hou'])
    data0 = data0.dropna().reset_index(drop=True)

    print(CITY, len(data0))

    process_row_partial = partial(home_process_row, graph=graph, CITY=CITY)

    with Pool(processes=processes_num) as pool:
        distances = pool.starmap(process_row_partial, data0.iterrows())

    distances_sorted = [distances[i] for i in range(len(distances))]
    data0['home_road_dist'] = distances_sorted
    data0['home_road_dist'] = (data0['home_road_dist'] / 1000).round(2)

    merged_data = pd.merge(data, data0, on=['grid_lon_hou', 'grid_lat_hou', 'POINT_X', 'POINT_Y'], how='inner')
    merged_data = merged_data.drop_duplicates(subset=['id', 'name'])

    merged_data.to_csv(PATH_RESULT2.joinpath(CITY + '剔除人数异常少的医院 加房价汇网格加路网.csv'), index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    starttime = time.time()

    CITY_LIST = ['']
    processes_num = 8
    for CITY in CITY_LIST:
        main(CITY)

    endtime = time.time()
    print("time consuming", (endtime - starttime) / 60, "minutes")


