import os
import pandas as pd
import configuration as config

def write_asset_as_empty(collection, day_to_process, remark):
    print('Cutting asset create for {} / {}'.format(collection, day_to_process))
    print('Reason: {}'.format(remark))
    collection_name = os.path.basename(collection)
    df = pd.DataFrame([(collection_name, day_to_process, remark)])
    df.to_csv(config.EMPTY_ASSET_LIST, mode='a', header=False, index=False)