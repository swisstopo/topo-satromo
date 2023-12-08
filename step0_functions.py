import os
import pandas as pd
import configuration as config
import ee
from datetime import datetime, timedelta
from step0_processor import generate_asset_mosaic_for_single_date

def step0_main(step0_product_dict, current_date_str):
    collections_ready = list()
    # We check every step0 collection independently
    # The collection is ready if all assets are present for the interval [date-temporal_coverage; date]
    for step0_collection, (products, temporal_coverage, base_collection) in step0_product_dict.items():
        temporal_coverage -= 1
        ok = step0_check_collection(step0_collection, temporal_coverage, current_date_str)
        if ok:
            collections_ready.append(step0_collection)

    return collections_ready



def step0_check_collection(collection, temporal_coverage, current_date_str):
    list_asset_response = ee.data.listAssets({'parent': collection})
    assets = list_asset_response['assets']
    target_date = datetime.strptime(current_date_str, "%Y-%m-%d").date()

    # asset_cleaning
    if 'cleaning_older_than' in config.step0[collection]:
        target_date = target_date + timedelta(days=-1 * config.step0[collection]['cleaning_older_than'])
        for asset in assets:
            date = asset['properties']['date']
            date_as_datetime = datetime.strptime(date, '%Y-%m-%d')
            if date_as_datetime < target_date:
                print('remove asset {}'.format(date))
                # ee.data.deleteAsset(assetId=asset['id']) TODO uncomment this line to actually delete the assets

    # Check that asset is present for every date of the temporal coverage
    check_date = target_date + timedelta(days=-1*temporal_coverage)
    end_date = target_date
    all_present = True
    while check_date <= end_date:
        asset_prepared = check_if_asset_prepared(collection, assets, check_date)
        if not asset_prepared:
            print('Asset missing for date {}'.format(check_date))
            all_present = False
        check_date += timedelta(days=1)

    return all_present

def check_if_asset_prepared(collection, assets, check_date):
    # 1. check if in asset list
    # 2. if not in asset list check if in empty_asset_list
    # 3. if not in the empty_asset_list, check if running tasks
    # 4. if not in running tasks, start task (if empty, write the empty_asset_list)
    check_date_str = check_date.strftime('%Y-%m-%d')
    print('checking date {}'.format(check_date))

    # 1. check if in asset list
    for asset in assets:
        asset_date = asset['properties']['date']
        if asset_date == check_date_str:
            print('Collection {} READY for date {}'.format(collection, check_date_str))
            return True
    print('Asset not found in custom collection, continuing...')

    # 2. if not in asset list check if in empty_asset_list
    df = pd.read_csv(config.EMPTY_ASSET_LIST)
    collection_basename = os.path.basename(collection)
    df_selection = df[(df.collection == collection_basename) & (df.date == check_date_str)]
    if len(df_selection) > 0:
        print('Date found in empty_asset_list, skipping date')
        return True

    tasks = ee.data.listOperations()
    task_description = collection_basename + '_' + check_date_str
    for task in tasks:
        if task['metadata']['description'] != task_description:
            continue
        if task['metadata']['state'] in ['PENDING', 'RUNNING']:
            print('task {} still running, skipping asset creation'.format(task_description))
            return False

    print('Starting asset generation for {} / {}'.format(collection, check_date_str))
    generate_asset_mosaic_for_single_date(check_date_str, collection, task_description)
    return False

def get_step0_dict():
    """
    This function is used to extract the step0 information from the config object and store it in a dictionary.
    The dictionary has the collection names as keys and the product names and temporal coverages as values
    """
    step0_dict = dict()
    for entry in dir(config):
        entry_value = getattr(config, entry)
        if not isinstance(entry_value, dict):
            continue
        if 'step0_collection' not in entry_value:
            continue
        temporal_coverage = int(entry_value['temporal_coverage'])
        collection_name = entry_value['step0_collection']
        base_collection = entry_value['image_collection']
        if collection_name not in step0_dict:
            step0_dict[collection_name] = [[entry, ], temporal_coverage, base_collection]
        else:
            if base_collection != step0_dict[collection_name][2]:
                raise BrokenPipeError('Inconsistent base collection in configuration file')

            temporal_coverage = max(step0_dict[collection_name][1], temporal_coverage)
            step0_dict[collection_name][0].append(entry)
            step0_dict[collection_name][1] = temporal_coverage

    return step0_dict

