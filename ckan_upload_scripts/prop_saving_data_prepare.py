import pandas as pd
import pdb

raw_data = pd.read_csv('/Users/shreyaagrawal/Downloads/data_with_tender_stage(1).csv')
data = raw_data.copy()

data['buyer_name'] = data.apply(lambda row: row["buyer/name"].split("||")[0], axis=1)
pdb.set_trace()
data['tender/value/amount'] = data['tender/value/amount'].str.replace(",","")
# data['awards/0/value/amount'] = data['awards/0/value/amount'].str.replace(",","")
# data['fiscal_year'] = data['tender/bidOpening/date'].map(lambda x: x.year if x.month > 3 else x.year-1)
# data['fiscal_year'] = data['fiscal_year'].map(lambda x: str(x) + "-" + str(x+1)[2:4])
data['saving/overrun'] = data['tender/value/amount'].astype(float) - data['awards/0/value/amount']
data['saving/overrun'] = data['saving/overrun'].map(lambda x: 'Saving' if x > 0 else 'Overrun' if x < 0 else 'Samevalue' )


data_grpd = data.groupby(['fiscal_year', 'buyer_name', 'tender/stage', 'tender/mainProcurementCategory', 'saving/overrun']).size().to_frame('tender_count').reset_index()
data_grpd.to_csv('prop_saving_16_23.csv')