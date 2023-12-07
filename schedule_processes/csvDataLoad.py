"""
This Process takes a csv file of hubspot records uploads to the database then updates hubspot.
"""
import DDIP_Library as ddip
from datetime import datetime
import os
import pandas as pd

load_dir = 'initial_load/'
sandbox = True

companies = f'{load_dir}test_company.csv'
# branches = f'{load_dir}branches.csv'
contacts = f'{load_dir}test_contact.csv'

# ----- Loading to database to start ---------
ddip.load_all(output_dir='initial_load/', company_file=companies)
# ddip.load_all(output_dir='initial_load/', branch_file=branches) ## Don't have the branch object in hubspot populated
ddip.load_all(output_dir='initial_load/', contact_file=contacts)

# ---- After database load verified load to sandbox -----------
companies_data = ddip.export_companies(return_list=True)
ddip.hubspot_api_inserting.update_hubspot_objects(object_type='companies',
                                                  objects_to_update=companies_data,
                                                  sandbox=sandbox)

branches_data = ddip.export_branches(return_list=True)
ddip.hubspot_api_inserting.update_hubspot_objects(object_type='branches',
                                                  objects_to_update=branches_data,
                                                  sandbox=sandbox)

contacts_data = ddip.export_contacts(return_list=True)
ddip.hubspot_api_inserting.update_hubspot_objects(object_type='contacts',
                                                  objects_to_update=contacts_data,
                                                  sandbox=sandbox)

# creates the new associations
ddip.hubspot_api_misc.set_contact_branch_associations()

ddip.hubspot_api_misc.set_branch_company_associations()
