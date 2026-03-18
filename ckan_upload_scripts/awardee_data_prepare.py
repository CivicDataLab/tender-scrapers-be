import pandas as pd
import pdb

raw_data = pd.read_csv('/Users/shreyaagrawal/Downloads/data_with_tender_stage(2).csv')
data = raw_data.copy()
pdb.set_trace()
data['buyer_name'] = data.apply(lambda row: row["buyer/name"].split("||")[0], axis=1)
# data['fiscal_year'] = data['tender/bidOpening/date'].map(lambda x: x.year if x.month > 3 else x.year-1)
# data['fiscal_year'] = data['fiscal_year'].map(lambda x: str(x) + "-" + str(x+1)[2:4])

data['competition'] = data['tender/numberOfTenderers'].map(lambda x: x -1)
data['awardee'] = data['awards/0/suppliers/0/name'].map(lambda x: x)


# data_grpd = data.groupby(['fiscal_year', 'buyer_name', 'tender/stage', 'tender/mainProcurementCategory', 'awardee']).size().to_frame('tender_count').reset_index()


#apply multiple functions on multiple columns of groupby example
def f(x):
    d = {}
    d['tender_count'] = len(x)
    d['avg_comp'] = x['competition'].mean()
    d['award_val'] = x['awards/0/value/amount'].sum()
    return pd.Series(d, index=['tender_count', 'avg_comp', 'award_val'])

data_grpd = data.groupby(['fiscal_year', 'buyer_name', 'tender/stage', 'tender/mainProcurementCategory', 'awardee']).apply(f).reset_index()
data_grpd.to_csv('awardee_details_16_20.csv')
