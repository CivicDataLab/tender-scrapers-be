import pandas as pd

award2016 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2016_OCDS_mapping_v01.xlsx', sheet_name = 'Sheet1')
award2017 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2017_OCDS_mapping_v01.xlsx', sheet_name = 'Sheet1')
award2018 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2018_OCDS_mapping_v01.xlsx', sheet_name = 'Sheet1')
award2019 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2019_OCDS_mapping_v01.xlsx', sheet_name = 'Sheet1')
award2020 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2020_OCDS_mapping_v01.xlsx', sheet_name = 'Sheet1')

tender2016 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2016_OCDS_mapping_v01.xlsx', sheet_name = 'data_2016_OCDS_mapping_v01')
tender2017 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2017_OCDS_mapping_v01.xlsx', sheet_name = 'data_2017_OCDS_mapping_v01')
tender2018 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2018_OCDS_mapping_v01.xlsx', sheet_name = 'data_2018_OCDS_mapping_v01')
tender2019 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2019_OCDS_mapping_v01.xlsx', sheet_name = 'data_2019_OCDS_mapping_v01')
tender2020 = pd.read_excel('/home/abhinav/OCI/Data/raw/OCDS Mapping/data_2020_OCDS_mapping_v01.xlsx', sheet_name = 'data_2020_OCDS_mapping_v01')

award2016.drop(['id', 'date'], axis=1, inplace=True)
award2017.drop(['id', 'date'], axis=1, inplace=True)
award2018.drop(['id', 'date'], axis=1, inplace=True)
award2019.drop(['id', 'date'], axis=1, inplace=True)
award2020.drop(['id', 'date'], axis=1, inplace=True)

final_data2016 = pd.merge(tender2016, award2016, on='ocid', how='left')
final_data2017 = pd.merge(tender2017, award2017, on='ocid', how='left')
final_data2018 = pd.merge(tender2018, award2018, on='ocid', how='left')
final_data2019 = pd.merge(tender2019, award2019, on='ocid', how='left')
final_data2020 = pd.merge(tender2020, award2020, on='ocid', how='left')

final_data2016 = final_data2016.fillna('')
final_data2017 = final_data2017.fillna('')
final_data2018 = final_data2018.fillna('')
final_data2019 = final_data2019.fillna('')
final_data2020 = final_data2020.fillna('')

final_data2016['fiscal_year'] = "2016-17"
final_data2017['fiscal_year'] = "2017-18"
final_data2018['fiscal_year'] = "2018-19"
final_data2019['fiscal_year'] = "2019-20"
final_data2020['fiscal_year'] = "2020-21"

final_data = final_data2016.append(final_data2017, ignore_index=True)
final_data = final_data.append(final_data2018, ignore_index=True)
final_data = final_data.append(final_data2019, ignore_index=True)
final_data = final_data.append(final_data2020, ignore_index=True)

final_data = final_data.iloc[:, 0:33]
final_data.to_excel('ocds_mapped_data_16_20.xlsx')
