# -*- coding: UTF-8 -*-
'''
Created on 26 sept. 2016

@author: dlrr
'''
from faker import Faker
from pandas import DataFrame, read_csv, merge

import pandas as pd
import random
import argparse
import datetime

import numpy as np
from _cffi_backend import string

class MainModule:
    
    def __init__(self):
        self.fake = Faker()
    
    def generate_csv(self, accounts, counterparty, collateral, tenors, average_num_counterparties_per_account, average_num_collateral_per_account):
        self.init_accounts(accounts)
        self.init_tenors(tenors)
        self.init_collateral(collateral)
        self.init_counterparty(int(counterparty))
        self.generateModelCoreCalc()
        self.generateModelOthRep()
        self.generateNonModelOthRep()
        self.generateModelTenorOutput()
        self.generateModelCollateral(average_num_collateral_per_account)
        self.generateCounterparty(average_num_counterparties_per_account)
      
        
    def init_accounts(self, accounts):
        __account_deal_id = [ self.fake.pystr(min_chars=None, max_chars=10) for _ in range(accounts) ]
        __facility_id = [ self.fake.pystr(min_chars=None, max_chars=10) for _ in range(accounts) ]    
        accountList = list(zip(__account_deal_id, __facility_id))
        self.dfAccount = pd.DataFrame(data=accountList, columns=['account_deal_id', 'facility_id'])
        self.dfAccount['config_id'] = self.config_id
        self.dfAccount["key"] = 0
        #print(self.dfAccount)
    
    def init_tenors(self, tenors):
        __tenors = [ i for i in range(tenors) ]
        tenorList = list(zip(__tenors))
        self.dfTenors = pd.DataFrame(data=tenorList, columns=['tenor_mth'])
        self.dfTenors["key"] = 0
        #print(self.dfTenors)
    
    def init_collateral(self, collateral):
        collateralList = [i  for i in range(collateral)]
        self.dfCollateral = pd.DataFrame(data=collateralList, columns=['collateral_id'])
        self.dfCollateral["key"] = 0
        #print(self.dfCollateral)
    
    def init_counterparty(self, counterparty):
        counterpartyList = [i for i in range(counterparty)]
        self.dfCounterParty = pd.DataFrame(data=counterpartyList, columns=['customer_id'])
        self.dfCounterParty["key"] = 0
        #print(self.dfCounterParty)
    
    def generateModelCoreCalc(self):
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
        self.dfModelCoreCalcOutputCsv = self.dfAccount.join(dfModelCoreCalcOutputRestColumns)
        del self.dfModelCoreCalcOutputCsv["key"]
        self.dfModelCoreCalcOutputCsv.to_csv('model_core_calc_output.csv', index = False)
        
    def generateModelOthRep(self):
        dfModelOthRepColumns = pd.DataFrame(columns=['drawn_gross_exp_amt',
                                                     'undrawn_gross_exp_amt', 'default_amt', 'default_date', 'default_reason',
                                                     'forbearance_entry_date', 'forbearance_ind', 'forbearance_type',
                                                     'transcation_ccy', 'account_number', 'btl_residential_ind', 'charge_off_amt',
                                                     'charge_off_date', 'charge_off_reason', 'off_balance_sheet_amt', 'off_balance_sheet_amt',
                                                     'off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt',
                                                     'off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt',
                                                     'off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt',
                                                     'off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt',
                                                     'off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt',
                                                     'off_balance_sheet_amt','off_balance_sheet_amt','off_balance_sheet_amt',
                                                     'number_attribute_1', 'number_attribute_2','number_attribute_3','number_attribute_4','number_attribute_5'
                                                     'number_attribute_6', 'number_attribute_7','number_attribute_8','number_attribute_9','number_attribute_10',
                                                     'number_attribute_11', 'number_attribute_12','number_attribute_13','number_attribute_14','number_attribute_15',
                                                     'number_attribute_16', 'number_attribute_17','number_attribute_18','number_attribute_19','number_attribute_20',
                                                     'number_attribute_21', 'number_attribute_22','number_attribute_23','number_attribute_24','number_attribute_25',
                                                     'date_attribute_1','date_attribute_2','date_attribute_3','date_attribute_4','date_attribute_5', 'date_attribute_6',
                                                     'date_attribute_7','date_attribute_8','date_attribute_9','date_attribute_10','date_attribute_1'])
        self.dfModelOthRepCsv = self.dfAccount.join(dfModelOthRepColumns)
        del self.dfModelOthRepCsv["key"]
        self.dfModelOthRepCsv.to_csv('model_oth_rep.csv', index = False)
        
    def generateNonModelOthRep(self):
        dfNonModelOthRep = pd.DataFrame(columns=['reg_exposure_type', 'transaction_ccy', 'banking_trading_ind', 'basel_approach_ind', 'bic_code', 'branch_write_off_amt',
                                                 'exchange_rate', 'gross_carying_amt', 'individual_collective_ind', 'sort_code', 'unit_type', 'recovered_asset_ind',
                                                 'recovery_amt', 'write_off_ind', 'write_off_date', 'debt_col_agency_fees_amt', ' debt_sale_proceeds_amt',
                                                 'feeds_in_suspence_amt', 'write_off_amt', 'pwor_amt', 'product_id', 'product_type', 'pwor_date', 'source_system_id',
                                                 'write_off_interest_amt', 'write_off_principle_amt', 'write_off_reason', 'balance_at_write_off_amt', 'balance_interest_amt',
                                                 'close_date', 'repossession_date', 'repossession_reason', 'statutory_unit_code', 'balance_principle_amt', 'close_date',
                                                 'repossesion_date', 'repossesion_reason', 'statutory_unit_code', 'balance_principle_amt', 'appears_amt', 'latest_drawdown_date',
                                                 'relationship_start_date', 'facility_open_date', 'initial_recognition_date', 'pay_type', 
                                                 'number_attribute_1', 'number_attribute_2', 'number_attribute_3', 'number_attribute_4', 'number_attribute_5', 
                                                 'number_attribute_6', 'number_attribute_7', 'number_attribute_8', 'number_attribute_9', 'number_attribute_10', 
                                                 'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5', 
                                                 'date_attribute_6', 'date_attribute_7', 'date_attribute_8', 'date_attribute_9', 'date_attribute_10', 
                                                 'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5' ])
        self.dfNonModelOthRepCsv = self.dfAccount.join(dfNonModelOthRep)
        del self.dfNonModelOthRepCsv["key"]
        self.dfNonModelOthRepCsv.to_csv('non_model_oth_rep.csv', index = False)
                
    def generateModelTenorOutput(self):
        dfModelTenorOuput = pd.DataFrame(columns=['ifrs9_pd_val', 'ifrs9_lgd_val',  'ifrs9_ead_val', 'scenario_type'])
        dfModeltenorOuputTenors = pd.merge(self.dfAccount, self.dfTenors, on='key')
        del dfModeltenorOuputTenors["key"]
        self.dfModeltenorOuputCsv = dfModeltenorOuputTenors.join(dfModelTenorOuput)
        #print(dfModeltenorOuputCsv)
        self.dfModeltenorOuputCsv.to_csv('model_tenor_output.csv', index = False)
        
    def generateModelCollateral(self, average_num_collateral_per_account):
        dfModelCollateralColumns = pd.DataFrame(columns=['reposession_date', 'repossesion_reason', 'ltv_origination_val', 'ltv_current_val',
                                                  'balance_at_origination_amt', 'original_valuation_amt', 'curr_hpl_valuation_amt',
                                                  'curr_avm_valuation_amt', 'number_attribute_1', 'number_attribute_2', 
                                                  'number_attribute_3', 'number_attribute_4', 'number_attribute_5',
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5', 
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5' ])
        #dfCollateralFiltered = self.dfCollateral[(self.dfCollateral.random < 0.1 )].copy()
        #collateralListFiltered = np.random.choice(self.collateralList,average_num_collateral_per_account)
        avg = average_num_collateral_per_account / len(self.dfCollateral)
        dfCollateralId = merge(self.dfAccount, self.dfCollateral, on="key")
        del dfCollateralId["key"]
        #print(dfCollateral)
        dfModelCollateral = dfCollateralId.sample(frac=avg, replace=True)
        #print(dfModelCollateral)
        dfModelCollateralCsv = dfModelCollateral.join(dfModelCollateralColumns)
        #dfModelCollateralCsvSorted = dfModelCollateralCsv.sort('account_deal_id', ascending = False)
        self.dfModelCollateralCsvSorted = dfModelCollateralCsv.sort_values(by='account_deal_id')
        #print(dfModelCollateralCsv)
        #print(dfModelCollateralCsvSorted)
        self.dfModelCollateralCsvSorted.to_csv('model_collateral.csv', index = False)

    def generateCounterparty(self, average_num_counterparties_per_account):
        dfModelCounterPartyColumns = pd.DataFrame(columns=['customer_name', 'country_risk_code', 'country_incorp_res_code', 'number_attribute_1', 'number_attribute_2', 
                                                  'number_attribute_3', 'number_attribute_4', 'number_attribute_5',
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5', 
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5'])
        avg = average_num_counterparties_per_account / len(self.dfCounterParty)
        dfCounterPartyId = merge(self.dfAccount, self.dfCounterParty, on='key')
        del dfCounterPartyId["key"]
        #print(dfCounterPartyId.count())
        dfModelCounterParty = dfCounterPartyId.sample(frac=avg, replace=True)
        dfModelCounterPartyCsv = dfModelCounterParty.join(dfModelCounterPartyColumns)
        #dfModelCounterPartyCsvSorted = dfModelCounterPartyCsv.sort('account_deal_id', ascending = False)
        self.dfModelCounterPartyCsvSorted = dfModelCounterPartyCsv.sort_values(by='account_deal_id')
        #print(dfModelCounterPartyCsv)
        #print(dfModelCounterPartyCsvSorted)
        self.dfModelCounterPartyCsvSorted.to_csv('model_counter_party.csv', index=False)
        
    def genterate_config_files(self, config_id, reporting_date, portfolio_id, sub_portfolio_id, cluster_id, reporting_ccy, eff_from_date, eff_to_date):
        self.config_id = config_id
        index = []
        index.append(0)
        dfModelConfig = pd.DataFrame(index=index, columns=['key'])
        dfModelConfig['config_id'] = config_id
        dfModelConfig['reporting_date'] = reporting_date
        dfModelConfig['portfolio_id'] = portfolio_id
        dfModelConfig['sub_portfolio_id'] = sub_portfolio_id
        dfModelConfig['cluster_id']= cluster_id
        dfModelConfig['reporting_ccy'] = reporting_ccy
        dfModelConfig['eff_from_date'] = eff_from_date
        dfModelConfig['eff_to_date'] = eff_to_date
        del dfModelConfig["key"]
        #print(dfModelConfig)
        dfModelConfig.to_csv('model_config.csv', index=False)
        dfModelConfig.to_csv('non_model_config.csv', index=False)  
    
    def generate_check_files(self):
        model_files = ['model_core_calc_output.csv', 'model_oth_rep.csv', 'model_tenor_output.csv','model_collateral.csv', 'model_counter_party.csv']
        model_num_rows = [len(self.dfModelCoreCalcOutputCsv), len(self.dfModelOthRepCsv), len(self.dfModeltenorOuputCsv), len(self.dfModelCollateralCsvSorted), len(self.dfModelCounterPartyCsvSorted) ]
            
        modelCheckList = list(zip(model_files, model_num_rows))
        dfModelCheck = pd.DataFrame(modelCheckList, columns=['file', 'num_rows'])
        #print(dfModelCheck)
        dfModelCheck.to_csv('model_check.csv', index=False)
        
        non_model_files = ['non_model_oth_rep.csv']
        non_model_num_rows = [len(self.dfNonModelOthRepCsv)]
        
        nonModelCheckList = list(zip(non_model_files, non_model_num_rows))
        dfNonModelCheck = pd.DataFrame(nonModelCheckList, columns=['file', 'num_rows'])
        print(dfNonModelCheck)
        dfModelCheck.to_csv('non_model_check.csv', index=False)
              
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Generate CSV with random values')
    parser.add_argument('--accounts', nargs='?', const=1, type=int, default=100,
                        help='account to generate default:100')
    parser.add_argument('--counterparties', nargs='?', const=1, type=int, default=10,
                        help='counterparty number (default: 10)')
    parser.add_argument('--collaterals', nargs='?', const=1, type=int, default=10,
                        help='collateral number (default: 10)')
    parser.add_argument('--tenors',nargs='?', const=1, type=int, default=6,
                        help='number of tenors (default: 6)')
    parser.add_argument('--average_num_counterparties_per_account', nargs='?', const=1, type=int, default=3,
                        help='average_num_counterparties_per_account  (default: 3)')
    parser.add_argument('--average_num_collateral_per_account', nargs='?', const=1, type=int, default=5,
                        help='average_num_collateral_per_account (default: 5)')
    
    #model_config
    parser.add_argument('--config_id', nargs='?', const=1, type=int, default=00,
                        help='config_id (default: 00)')
    parser.add_argument('--reporting_date', nargs='?', const=1, type=str, default=datetime.date.today(),
                        help='reporting_date (default: today)')
    parser.add_argument('--portfolio_id', nargs='?', const=1, type=str, default=np.NaN,
                        help='portfolio_id (default: NaN)')
    parser.add_argument('--sub_portfolio_id', nargs='?', const=1, type=str, default=np.NaN,
                        help='sub_portfolio_id (default: NaN)')
    parser.add_argument('--cluster_id', nargs='?', const=1, type=str, default=np.NaN,
                        help='cluster_id (default: NaN)')
    parser.add_argument('--reporting_ccy', nargs='?', const=1, type=str, default=np.NaN,
                        help='reporting_ccy (default: NaN)')
    parser.add_argument('--effective_from_date', nargs='?', const=1, type=str, default=np.NaN,
                        help='effective_from_date (default: NaN)')
    parser.add_argument('--effective_to_date', nargs='?', const=1, type=str, default=np.NaN,
                        help='effective_to_date (default: NaN)')
        
    print('Starting')
    args = []
    args = parser.parse_args()
    print(args)
    
    if args.config_id == 00:    
        fake = Faker()
        args.config_id = fake.pydecimal(left_digits=None, right_digits=None, positive=True)
    
    mainModule = MainModule()
   
    mainModule.genterate_config_files(args.config_id, args.reporting_date, args.portfolio_id, args.sub_portfolio_id, args.cluster_id,
                            args.reporting_ccy, args.effective_from_date, args.effective_to_date)
     
    mainModule.generate_csv(args.accounts, args.counterparties, args.collaterals, args.tenors, 
                            args.average_num_counterparties_per_account, args.average_num_collateral_per_account)
    
    mainModule.generate_check_files()
    print('Done')
    pass
