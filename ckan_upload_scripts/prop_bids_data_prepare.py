import pandas as pd
import pdb

raw_data = pd.read_csv('/Users/shreyaagrawal/Downloads/data_with_tender_stage - data_with_tender_stage.csv')
data = raw_data.copy()

data['buyer_name'] = data.apply(lambda row: row["buyer/name"].split("||")[0], axis=1)
# data['fiscal_year'] = data['tender/bidOpening/date'].map(lambda x: x.year if x.month > 3 else x.year-1)
# data['fiscal_year'] = data['fiscal_year'].map(lambda x: str(x) + "-" + str(x+1)[2:4])
pdb.set_trace()
data['number_of_bids'] = data['tender/numberOfTenderers'].map(lambda x: (str(x) + " bids") if int(x) < 4 else "4 and more bids")


data_grpd = data.groupby(['fiscal_year', 'buyer_name', 'tender/stage', 'tender/mainProcurementCategory', 'number_of_bids']).size().to_frame('tender_count').reset_index()
data_grpd.to_csv('prop_bids_16_20.csv')


