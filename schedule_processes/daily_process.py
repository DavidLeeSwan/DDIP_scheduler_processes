"""
This process is to take hubspot records created/modified in the past day and insert them into the database and then
re-upload the contacts into hubspot with new formatted fields and potential associations.
"""

import DDIP_Library as ddip
from datetime import datetime
import os

output_str = f'DDIP_daily_run_{datetime.today().strftime("%Y-%m-%d")}/'

try:
    os.makedirs(output_str, exist_ok=True)
    print(f"Directory '{output_str}' created successfully or already exists.")
except OSError as e:
    print(f"Error creating directory '{output_str}': {e}")

sandbox_get = False
sandbox_load = True
days_ago = 1


def daily_process():
    # pulls from hubspot
    ddip.api_calls.hubspot_api_extracting.get_day_old_record(record_type='companies',
                                                             sandbox=sandbox_get,
                                                             num_days=days_ago,
                                                             output_dir=output_str)

    ddip.api_calls.hubspot_api_extracting.get_day_old_record(record_type='branches',
                                                             sandbox=sandbox_get,
                                                             num_days=days_ago,
                                                             output_dir=output_str)

    ddip.api_calls.hubspot_api_extracting.get_day_old_record(record_type='contacts',
                                                             sandbox=sandbox_get,
                                                             num_days=days_ago,
                                                             output_dir=output_str)

    # ---------------- Companies ------------------
    # loads to the data table
    if os.path.exists(f'{output_str}new_companies.csv'):
        ddip.table_scrub.company_scrub(filepath=f'{output_str}new_companies.csv',
                                       errored_file=False,
                                       output_dir=output_str)

    if os.path.exists(f'{output_str}scrubbed_new_companies.csv'):
        ddip.newinsert.load_all(company_file=f'{output_str}scrubbed_new_companies.csv',
                                output_dir=output_str)

    # scrubs other database tables to try and get missing info from errored entries
    if os.path.exists(f'{output_str}Errored_Companies_{datetime.today().strftime("%Y-%m-%d")}.csv'):
        ddip.table_scrub.company_scrub(
            filepath=f'{output_str}Errored_Companies_{datetime.today().strftime("%Y-%m-%d")}.csv',
            output_dir=output_str)

    if os.path.exists(f'{output_str}reload_companies.csv'):
        ddip.newinsert.load_all(company_file=f'{output_str}reload_companies.csv',
                                output_dir=output_str)

    # ---------------- Branches ------------------
    # loads to the data table
    if os.path.exists(f'{output_str}new_branches.csv'):
        ddip.table_scrub.branch_scrub(filepath=f'{output_str}new_branches.csv',
                                      errored_file=False,
                                      output_dir=output_str)

    if os.path.exists(f'{output_str}scrubbed_new_branches.csv'):
        ddip.newinsert.load_all(branch_file=f'{output_str}scrubbed_new_branches.csv',
                                output_dir=output_str)
    # scrubs other database tables to try and get missing info from errored entries
    if os.path.exists(f'{output_str}Errored_Branches_{datetime.today().strftime("%Y-%m-%d")}.csv'):
        ddip.table_scrub.branch_scrub(filepath=f'{output_str}Errored_Branches_{datetime.today().strftime("%Y-%m-%d")}.csv',
                                      output_dir=output_str)

    if os.path.exists(f'{output_str}reload_branches.csv'):
        ddip.newinsert.load_all(branch_file=f'{output_str}reload_branches.csv',
                                output_dir=output_str)

    # ---------------- Contacts ------------------
    # loads to the data table
    if os.path.exists(f'{output_str}new_contacts.csv'):
        ddip.table_scrub.contact_scrub(filepath=f'{output_str}new_contacts.csv',
                                       output_dir=output_str)
        ddip.newinsert.load_all(contact_file=f'{output_str}new_contacts.csv',
                                output_dir=output_str)

    if os.path.exists(f'{output_str}scrubbed_new_contacts.csv'):
        ddip.newinsert.load_all(contact_file=f'{output_str}scrubbed_new_contacts.csv',
                                output_dir=output_str)
    # scrubs other database tables to try and get missing info from errored entries
    if os.path.exists(f'{output_str}Errored_Contacts_{datetime.today().strftime("%Y-%m-%d")}.csv'):
        ddip.table_scrub.contact_scrub(
            filepath=f'{output_str}Errored_Contacts_{datetime.today().strftime("%Y-%m-%d")}.csv',
            output_dir=output_str)

    if os.path.exists(f'{output_str}reload_contacts.csv'):
        ddip.newinsert.load_all(contact_file=f'{output_str}reload_contacts.csv',
                                output_dir=output_str)

    # ---------------- All loads ------------------
    # get updated table entries and uploads back to hubspot
    companies = ddip.extract.export_companies(return_list=True)
    ddip.api_calls.hubspot_api_inserting.update_hubspot_objects(object_type='companies',
                                                                objects_to_update=companies,
                                                                sandbox=sandbox_load)

    branches = ddip.extract.export_branches(return_list=True)
    ddip.api_calls.hubspot_api_inserting.update_hubspot_objects(object_type='branches',
                                                                objects_to_update=branches,
                                                                sandbox=sandbox_load)

    contacts = ddip.extract.export_contacts(return_list=True)
    ddip.api_calls.hubspot_api_inserting.update_hubspot_objects(object_type='contacts',
                                                                objects_to_update=contacts,
                                                                sandbox=sandbox_load)

    # creates the new associations
    ddip.api_calls.hubspot_api_misc.set_contact_branch_associations(output_dir=output_str)

    ddip.api_calls.hubspot_api_misc.set_branch_company_associations(output_dir=output_str)


if __name__ == '__main__':
    daily_process()
