"""
This Process takes a csv file of hubspot records uploads to the database then updates hubspot.
"""
import DDIP_Library as ddip
from datetime import datetime
import os
import pandas as pd

load_dir = 'initial_load/'
sandbox = True

company_load_path = f'{load_dir}company_test.csv'
# branch_load_path = f'{load_dir}branches.csv'
contact_load_path = f'{load_dir}contact_test.csv'

sandbox_load = True
days_ago = 1
output_str = 'initial_load/results/'


def csvDataLoad(company_file: str = None, branch_file: str = None, contact_file: str = None):
    # ---------------- Companies ------------------
    # loads to the data table
    if company_file is not None:
        print('Starting Load of:' , company_file)
        if os.path.exists(company_file):
            ddip.table_scrub.company_scrub(filepath=company_file, errored_file=False, output_dir=output_str)
        if os.path.exists(f'{output_str}scrubbed_new_companies.csv'):
            ddip.newinsert.load_all(company_file=f'{output_str}scrubbed_new_companies.csv', output_dir=output_str)
        # scrubs other database tables to try and get missing info from errored entries
        if os.path.exists(f'{output_str}Errored_Companies_{datetime.today().strftime("%Y-%m-%d")}.csv'):
            ddip.table_scrub.company_scrub(
                filepath=f'{output_str}Errored_Companies_{datetime.today().strftime("%Y-%m-%d")}.csv',
                output_dir=output_str)
        if os.path.exists(f'{output_str}reload_companies.csv'):
            ddip.newinsert.load_all(company_file=f'{output_str}reload_companies.csv', output_dir=output_str)

    # ---------------- Branches ------------------
    # loads to the data table
    if branch_file is not None:
        print('Starting Load of:', branch_file)
        if os.path.exists(branch_file):
            ddip.table_scrub.branch_scrub(filepath=branch_file, errored_file=False, output_dir=output_str)
        if os.path.exists(f'{output_str}scrubbed_new_branches.csv'):
            ddip.newinsert.load_all(branch_file=f'{output_str}scrubbed_new_branches.csv', output_dir=output_str)
        # scrubs other database tables to try and get missing info from errored entries
        if os.path.exists(f'{output_str}Errored_Branches_{datetime.today().strftime("%Y-%m-%d")}.csv'):
            ddip.table_scrub.branch_scrub(
                filepath=f'{output_str}Errored_Branches_{datetime.today().strftime("%Y-%m-%d")}.csv',
                output_dir=output_str)
        if os.path.exists(f'{output_str}reload_branches.csv'):
            ddip.newinsert.load_all(branch_file=f'{output_str}reload_branches.csv', output_dir=output_str)

    # ---------------- Contacts ------------------
    # loads to the data table
    if contact_file is not None:
        print('Starting Load of:', contact_file)
        if os.path.exists(contact_file):
            ddip.table_scrub.contact_scrub(filepath=contact_file, output_dir=output_str)
            ddip.newinsert.load_all(contact_file=contact_file, output_dir=output_str)
        if os.path.exists(f'{output_str}scrubbed_new_contacts.csv'):
            ddip.newinsert.load_all(contact_file=f'{output_str}scrubbed_new_contacts.csv', output_dir=output_str)
        # scrubs other database tables to try and get missing info from errored entries
        if os.path.exists(f'{output_str}Errored_Contacts_{datetime.today().strftime("%Y-%m-%d")}.csv'):
            ddip.table_scrub.contact_scrub(
                filepath=f'{output_str}Errored_Contacts_{datetime.today().strftime("%Y-%m-%d")}.csv', output_dir=output_str)
        if os.path.exists(f'{output_str}reload_contacts.csv'):
            ddip.newinsert.load_all(contact_file=f'{output_str}reload_contacts.csv', output_dir=output_str)

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
    ddip.api_calls.hubspot_api_misc.set_contact_branch_associations()

    ddip.api_calls.hubspot_api_misc.set_branch_company_associations()


if __name__ == '__main__':
    csvDataLoad(company_file=company_load_path, contact_file=contact_load_path)
