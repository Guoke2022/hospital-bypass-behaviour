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

starttime = time.time()

CITY_LIST = ['齐齐哈尔市', '成都市', '武汉市', '深圳市', '上海市', '广州市', '北京市']

names_to_exclude = []

for CITY in CITY_LIST:
    PATH_SHP = Path(f"...")
    PATH_RESULT0 = Path(f"...")
    PATH_RESULT2 = Path(f"...")
    PATH_RESULT4 = Path(f"...")
    PATH_RESULT5 = Path(f"...")

    # TODO：remove hospitals with abnormally low numbers of patients
    data = pd.read_csv(PATH_RESULT0.joinpath(f'{CITY}职住求距标签 已去重.csv'), encoding='utf-8')
    data_select = data.copy()
    data_select['count_1'] = 1
    hospital_count = data_select.groupby('name')['count_1'].sum()
    names_to_exclude.extend(hospital_count[hospital_count < 151].index.tolist())
    data_select = data_select[~data_select['name'].isin(names_to_exclude)]
    print(len(data_select))
    data_select.to_csv(PATH_RESULT2.joinpath(CITY + '剔除人数异常少的医院.csv'), index=False, encoding='utf-8-sig')
    excluded_hospitals = pd.DataFrame({'excluded_hospitals': names_to_exclude})
    excluded_hospitals.to_csv(PATH_RESULT2.joinpath(CITY + '剔除人数异常少的医院列表.csv'), index=False,
                              encoding='utf-8-sig')

# --------------------------------------Interpolating housing prices in ArcGIS----------------------------------------

# ------------------------------------------------------Finished------------------------------------------------------

# TODO:Connect the original data with the interpolation results of housing prices
CITY_LIST = ['齐齐哈尔市', '西安市', '成都市', '武汉市', '深圳市', '上海市', '广州市', '北京市']

for city in CITY_LIST:
    PATH_SHP = Path(f"...")
    df_hospital = pd.read_csv(PATH_SHP.joinpath(f'{city}_医疗机构_gcj02新.csv'), encoding='utf-8')
    PATH_RESULT0 = Path(f"...")
    PATH_RESULT2 = Path(f"...")
    data0 = pd.read_csv(PATH_RESULT0.joinpath(f'{city}已插值.csv'), encoding='utf-8')
    data0['HousePrice'] = data0['HousePrice'].fillna(0)

    data_label = pd.read_csv(PATH_RESULT2.joinpath(city + '剔除人数异常少的医院.csv'), encoding='utf-8')
    data_label_merge = pd.merge(left=data_label, right=data0[['id', 'gird_id', 'HousePrice']], on='id', how='inner')
    data_label_merge = data_label_merge.drop_duplicates(subset=['id', 'name'])
    data_label_merge = data_label_merge.reset_index()
    print(city, len(data_label_merge))

    if data_label_merge.columns[0] == 'index':
        data_label_merge = data_label_merge.iloc[:, 1:]

    # TODO:Using grid center point coordinates as approximate residential coordinates
    grouped = data_label_merge.groupby('gird_id')[['lon_housing', 'lat_housing']].mean()
    print(city, len(grouped))
    data_label_merge = data_label_merge.merge(grouped, on='gird_id', suffixes=('', '_mean'))
    data_label_merge.rename(columns={'lon_housing_mean': 'grid_lon_hou', 'lat_housing_mean': 'grid_lat_hou'}, inplace=True)

    data_label_merge = pd.merge(left=data_label_merge, right=df_hospital[['name', 'POINT_X', 'POINT_Y']], on='name', how='inner')
    data_label_merge.to_csv(PATH_RESULT2.joinpath(city + '剔除人数异常少的医院 加房价汇网格.csv'), index=False, encoding='utf-8-sig')

endtime = time.time()
print("time consuming", (endtime - starttime) / 60, "minutes")


