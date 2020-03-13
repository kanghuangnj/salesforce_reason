from lib import Longterm
from lib.header import CACHEPATH
import os
pj = os.path.join

def hot_location_longterm():
    sources = ['opportunity', 'building']
    reason = Longterm(sources)
    hot_df = reason.export()
    hot_df.to_csv(pj(CACHEPATH, 'hot_location.csv'))
    return hot_df