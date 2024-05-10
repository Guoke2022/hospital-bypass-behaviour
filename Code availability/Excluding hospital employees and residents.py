import pandas as pd
import time
from haversine import haversine, Unit
from pathlib2 import Path
import geopandas as gpd
from shapely.geometry import Point
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

starttime = time.time()

def f_haversine(lat1, lon1, lat2, lon2):
    point1 = (lat1, lon1)
    point2 = (lat2, lon2)
    distance = haversine(point1, point2, unit=Unit.METERS)

    return distance

# TODO: Parameter settings
DAY_NUM = 61
JUDGE_RESIDENT_NUM = 30  # one-second
JUDGE_WORKER_NUM = 20  # one-third

CITY_LIST = ['齐齐哈尔市', '海东市', '普洱市', '日喀则市', '吐鲁番市', '成都市', '武汉市', '深圳市', '上海市', '广州市', '北京市']

for CITY in CITY_LIST:
    PATH_RESULT1_ = f"..."
    PATH_RESULT2 = f"..."
    PATH_RESULT3 = f"..."
    PATH_SHP = f"..."

    polygonShp = gpd.read_file(PATH_SHP)

    INPUT_PATH = Path(PATH_RESULT1_)
    target_str2 = "潜在单日就医用户（已计算停留时间）"
    dfs2 = []

    for file_path in INPUT_PATH.glob('*'):
        if target_str2 in file_path.name:
            df = pd.read_csv(file_path)
            dfs2.append(df)

    merged_df2 = pd.concat(dfs2, axis=0, ignore_index=True)

    Count_num_max = JUDGE_WORKER_NUM
    id_counts = merged_df2.groupby('id').size()
    df_id_counts_result = merged_df2[merged_df2['id'].isin(id_counts[id_counts >= Count_num_max].index)]

    working_point = pd.DataFrame(columns=['id', 'lon', 'lat'])
    for id in df_id_counts_result["id"].unique():
        df_center = df_id_counts_result[df_id_counts_result["id"] == id].copy()
        center_lon = df_center['lon'].mean()
        center_lat = df_center['lat'].mean()
        working_point = working_point.append({'id': id, 'lon': center_lon, 'lat': center_lat}, ignore_index=True)

    Path_working_point = Path(PATH_RESULT2).joinpath(CITY + '医院职工用户名单（次数判断）' + '.csv')
    working_point.to_csv(Path_working_point, index=False, encoding='utf-8-sig')

    Path_working_point = Path(PATH_RESULT2).joinpath(CITY + '医院职工用户名单（次数判断）' + '.csv')
    working_point = pd.read_csv(Path_working_point, encoding='utf-8')

    data_jk = pd.read_csv(f'...', dtype={'类型': 'category', '省份': 'category', '城市': 'category', '区县': 'category'},
                          encoding='utf-8')
    data_jk = data_jk.drop_duplicates()

    housing_list_jk = data_jk[data_jk['类型'] == '居住地']
    working_list_jk = data_jk[data_jk['类型'] == '工作地']

    INPUT_PATH = Path(PATH_RESULT1_)
    target_str3 = "潜在单日就医用户（已计算停留时间）"
    dfs3 = []

    for file_path in INPUT_PATH.glob('*'):
        if target_str3 in file_path.name:
            df_candidate = pd.read_csv(file_path)
            df_candidate = df_candidate[~df_candidate['id'].isin(working_point['id'])]
            df_candidate = pd.merge(left=df_candidate, right=housing_list_jk, how='left', on='id')
            df_candidate = pd.merge(left=df_candidate, right=working_list_jk, how='left', on='id')

            # Space connection
            df_checking = df_candidate[['id', 'lon', 'lat', 'name', 'lon_housing', 'lat_housing', 'lon_working', 'lat_working']]
            df_checking.columns = ['id', 'lon', 'lat', 'hospital_name', 'lon_housing', 'lat_housing',
                                   'lon_working', 'lat_working']
            geometry_housing = [Point(lon_housing, lat_housing) for lon_housing, lat_housing in zip(
                df_checking['lon_housing'], df_checking['lat_housing'])]
            gdf_points_housing = gpd.GeoDataFrame(df_checking, geometry=geometry_housing)
            gdf_points_housing = gpd.sjoin(gdf_points_housing, polygonShp, how="inner", op="within")[
                ['id', 'hospital_name', 'name']]
            gdf_points_housing.columns = ['id', 'hospital_name', 'housing_name']

            gdf_points_housing_delete = gdf_points_housing[
                gdf_points_housing['housing_name'] == gdf_points_housing['hospital_name']]
            df_checking = df_checking.drop(index=df_checking.index[gdf_points_housing_delete.index])
            df_checking = df_checking.reset_index(drop=True)

            geometry_working = [Point(lon_working, lat_working) for lon_working, lat_working in zip(
                df_checking['lon_working'], df_checking['lat_working'])]
            gdf_points_working = gpd.GeoDataFrame(df_checking, geometry=geometry_working)
            gdf_points_working = gpd.sjoin(gdf_points_working, polygonShp, how="inner", op="within")[
                ['id', 'hospital_name', 'name']]
            gdf_points_working.columns = ['id', 'hospital_name', 'working_name']

            gdf_points_working_delete = gdf_points_working[
                gdf_points_working['working_name'] == gdf_points_working['hospital_name']]
            df_checking = df_checking.drop(index=df_checking.index[gdf_points_working_delete.index])
            df_checking = df_checking.reset_index(drop=True)

            # TODO:Filter out the excluded IDs from the original file
            target_user = pd.merge(df_candidate, df_checking[['id']], how='right', on=["id"])
            print(file_path.name, len(target_user))

            new_file_name = file_path.stem.replace('潜在单日就医用户（已计算停留时间）', '单日就医用户 带职住_final')
            Path_df_final = Path(PATH_RESULT3).joinpath(new_file_name + '.csv')
            target_user.to_csv(Path_df_final, index=False, encoding='utf-8-sig')

endtime = time.time()
print("time consuming:", (endtime - starttime) / 60, "minutes")


