import pandas as pd
import pdb

raw_data = pd.read_csv('/Users/shreyaagrawal/Downloads/data_with_tender_stage - data_with_tender_stage.csv')
data = raw_data.copy()
pdb.set_trace()
data['buyer_name'] = data.apply(lambda row: row["buyer/name"].split("||")[0], axis=1)
# data['fiscal_year'] = data['tender/bidOpening/date'].map(lambda x: x.year if x.month > 3 else x.year-1)
# data['fiscal_year'] = data['fiscal_year'].map(lambda x: str(x) + "-" + str(x+1)[2:4])


data_grpd = data.groupby(['fiscal_year', 'buyer_name', 'tender/stage', 'tender/mainProcurementCategory', 'tender/procurementMethod']).size().to_frame('tender_count').reset_index()
data_grpd.to_csv('prop_procmethod_16_23.csv')

