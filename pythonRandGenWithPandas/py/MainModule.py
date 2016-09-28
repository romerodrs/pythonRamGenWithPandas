'''
Created on 26 sept. 2016

@author: dlrr
'''
from faker import Faker
from pandas import DataFrame, read_csv

import pandas as pd
import random

class MainModule:
    # self.
    def __init__(self):
        print('Starting')
        self.fake = Faker()
        accounts = 10
        tenors = 10
        collateral = 10
        counterparty = 10
        self.init_config_id()
        self.init_accounts(accounts)
        self.init_tenors(tenors)
        self.init_collateral(collateral)
        self.init_counterparty(counterparty)
        self.generateModelCoreCalcOutput()
        print('Done')
      
    def init_config_id(self):
        self.config_id = self.fake.pydecimal(left_digits=None, right_digits=None, positive=True)
        #print(self.dfConfigId)
        
    def init_accounts(self, accounts):
        __account_deal_id = []
        __facility_id = []
        for _ in range(accounts):
            __account_deal_id.append(self.fake.pystr(min_chars=None, max_chars=10))
            __facility_id.append(self.fake.pystr(min_chars=None, max_chars=10))
        accountList = list(zip(__account_deal_id, __facility_id))
        self.dfAccount = pd.DataFrame(data=accountList, columns=['account_deal_id', 'facility_id'])
        self.dfAccount['config_id'] = self.config_id
        print(self.dfAccount)
    
    def init_tenors(self, tenors):
        __tenors = []
        for i in range(tenors):
            __tenors.append(i)
        tenorList = list(zip(__tenors))
        self.dfTenors = pd.DataFrame(data=tenorList, columns=['tenor_mth'])
        #print(self.dfTenors)
    
    def init_collateral(self, collateral):
        __collateral = []
        __collateral_alpha = []
        for i in range(collateral):
            __collateral.append(i)
            __collateral_alpha.append(random.uniform(0,1))
        collateralList = list(zip(__collateral, __collateral_alpha))
        self.dfCollateral = pd.DataFrame(data=collateralList, columns=['collateral_id', 'random'])
        #print(self.dfCollateral)
    
    def init_counterparty(self, counterparty):
        __counterparty = []
        __counterparty_alpha = []
        for i in range(counterparty):
            __counterparty.append(i)
            __counterparty_alpha.append(random.uniform(0,1))
        counterpartyList = list(zip(__counterparty, __counterparty_alpha))
        self.dfCounterParty = pd.DataFrame(data=counterpartyList, columns=['customer_id', 'random'])
        #print(self.dfCounterParty)
    
    def generateModelCoreCalcOutput(self):
        dfModelCoreCalcOutputRestColumns = pd.DataFrame(columns=['m12_pd_initial_recog_val',
                                                 'lifetime_pd_initial_recog_val','m12_pd_current_val','lifetime_pd_current_val',
                                                 'past_due_days','orig_barclays_grade_code','curr_barclays_grade_code',
                                                 'watchlist_category_code','hram_code','default_ind','cured_ind',
                                                 'max_contract_date','expected_mat_date','effective_interest_rate',
                                                 'credit_adj_eff_int_rate','number_attribute_1','number_attribute_2',
                                                 'number_attribute_3','number_attribute_4','number_attribute_5',
                                                 'number_attribute_6','number_attribute_7','number_attribute_8',
                                                 'number_attribute_9','number_attribute_10','data_attribute_11',
                                                 'data_attribute_1','data_attribute_2','data_attribute_3',
                                                 'data_attribute_4','data_attribute_5','data_attribute_6',
                                                 'data_attribute_7','data_attribute_8','data_attribute_9'])
        dfModelCoreCalcOutput = self.dfAccount.join(dfModelCoreCalcOutputRestColumns)
        dfModelCoreCalcOutput.to_csv('model_core_calc_output.csv', index = False)
        
if __name__ == '__main__':
    mainModule = MainModule()
    pass
