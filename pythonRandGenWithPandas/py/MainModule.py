# -*- coding: UTF-8 -*-
'''
Created on Oct2016

@author: Romero.Drs
'''
import argparse
from datetime import datetime

#from faker import Faker
from pandas import  merge

import random
import string
import numpy as np
import pandas as pd

class MainModule:
    
    def __init__(self, rdate):
        #self.fake = Faker()
        self.rdate = rdate
    
    def random_string_generator(self):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(10))
    
    def random_int_generator(self):
        return random.randrange(999999)
    
    def generate_csv(self, acc, cp, c, tenors, average_num_cp_per_account, average_num_c_per_account, sc):
        self.init_acc(acc)
        self.init_t(tenors)
        self.init_sc(sc)
        self.init_c(c)
        self.init_cp(int(cp))
        self.generateMCC()
        self.generateMOR()
        self.generateNMOR()
        self.generateMTO()
        self.generateMC(average_num_c_per_account)
        self.generateCP(average_num_cp_per_account)
      
        
    def init_acc(self, acc):
        accdid = [ self.random_string_generator() for _ in range(acc) ]
        fid = [ self.random_string_generator() for _ in range(acc) ]
        accList = list(zip(accdid, fid))
        self.dfAcc = pd.DataFrame(data=accList, columns=['acc_id', 'fc_id'])
        self.dfAcc['c_id'] = self.cid
        self.dfAcc["key"] = 0
        #print(self.dfAcc)
    
    def init_t(self, t):
        tList = [ i for i in range(t) ]
        self.dfT = pd.DataFrame(data=tList, columns=['t'])
        self.dfT["key"] = 0
        #print(self.dfT)
        
    def init_sc(self, sc):
        self.dfSc = pd.DataFrame(data=sc, columns=['sc'])
        self.dfSc["key"] = 0
        #print(self.dfSc)
    
    def init_c(self, c):
        cList = [i  for i in range(c)]
        self.dfC = pd.DataFrame(data=cList, columns=['c_id'])
        self.dfC["key"] = 0
        #print(self.dfC)
    
    def init_cp(self, cp):
        cpList = [i for i in range(cp)]
        self.dfCP = pd.DataFrame(data=cpList, columns=['cus_id'])
        self.dfCP["key"] = 0
        #print(self.dfCP)
    
    def generateMCC(self):
        dfColumns = pd.DataFrame(columns=['number_attribute_1','number_attribute_2',
                                                 'number_attribute_3','number_attribute_4','number_attribute_5',
                                                 'number_attribute_6','number_attribute_7','number_attribute_8',
                                                 'number_attribute_9','number_attribute_10','data_attribute_11',
                                                 'data_attribute_1','data_attribute_2','data_attribute_3',
                                                 'data_attribute_4','data_attribute_5','data_attribute_6',
                                                 'data_attribute_7','data_attribute_8','data_attribute_9'])
        self.dfMCCOCsv = self.dfAcc.join(dfColumns)
        del self.dfMCCOCsv["key"]
        self.dfMCCOCsv.to_csv('mcco_'+self.rdate+'.csv', index = False)
        
    def generateMOR(self):
        dfColumns = pd.DataFrame(columns=['number_attribute_1', 'number_attribute_2','number_attribute_3','number_attribute_4','number_attribute_5'
                                                     'number_attribute_6', 'number_attribute_7','number_attribute_8','number_attribute_9','number_attribute_10',
                                                     'number_attribute_11', 'number_attribute_12','number_attribute_13','number_attribute_14','number_attribute_15',
                                                     'number_attribute_16', 'number_attribute_17','number_attribute_18','number_attribute_19','number_attribute_20',
                                                     'number_attribute_21', 'number_attribute_22','number_attribute_23','number_attribute_24','number_attribute_25',
                                                     'date_attribute_1','date_attribute_2','date_attribute_3','date_attribute_4','date_attribute_5', 'date_attribute_6',
                                                     'date_attribute_7','date_attribute_8','date_attribute_9','date_attribute_10','date_attribute_1'])
        self.dfMORCsv = self.dfAcc.join(dfColumns)
        del self.dfMORCsv["key"]
        self.dfMORCsv.to_csv('mor_'+self.rdate+'.csv', index = False)
        
    def generateNMOR(self):
        dfColumns = pd.DataFrame(columns=['number_attribute_1', 'number_attribute_2', 'number_attribute_3', 'number_attribute_4', 'number_attribute_5', 
                                                 'number_attribute_6', 'number_attribute_7', 'number_attribute_8', 'number_attribute_9', 'number_attribute_10', 
                                                 'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5', 
                                                 'date_attribute_6', 'date_attribute_7', 'date_attribute_8', 'date_attribute_9', 'date_attribute_10', 
                                                 'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5' ])
        self.dfNMORCsv = self.dfAcc.join(dfColumns)
        del self.dfNMORCsv["key"]
        self.dfNMORCsv.to_csv('nmor_'+self.rdate+'.csv', index = False)
                
    def generateMTO(self):
        dfColumns = pd.DataFrame(columns=['val_1', 'val_2',  'val_3'])
        dfMOT = pd.merge(pd.merge(self.dfAcc, self.dfT, on='key'), self.dfSc, on='key')
        #print( dfMOT )
        del dfMOT["key"]
        self.dfMTOCsv = dfMOT.join(dfColumns) 
        #print(self.dfMTOCsv)
        self.dfMTOCsv.to_csv('mto_'+self.rdate+'.csv', index = False)
        
    def generateMC(self, average_num_c_per_account):
        dfColumns = pd.DataFrame(columns=['number_attribute_1', 'number_attribute_2', 
                                                  'number_attribute_3', 'number_attribute_4', 'number_attribute_5',
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5', 
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5' ])
        #dfCollateralFiltered = self.dfC[(self.dfC.random < 0.1 )].copy()
        #collateralListFiltered = np.random.choice(self.collateralList,average_num_c_per_account)
        avg = average_num_c_per_account / len(self.dfC)
        dfCid = merge(self.dfAcc, self.dfC, on="key")
        del dfCid["key"]
        #print(dfC)
        dfMC = dfCid.sample(frac=avg, replace=True)
        #print(dfMC)
        dfMCCsv = dfMC.join(dfColumns)
        #dfMCCsvSorted = dfMCCsv.sort('account_deal_id', ascending = False)
        self.dfMCCsvSorted = dfMCCsv.sort_values(by='acc_id')
        #print(dfMCCsv)
        #print(dfMCCsvSorted)
        self.dfMCCsvSorted.to_csv('mc_'+self.rdate+'.csv', index = False)

    def generateCP(self, average_num_cp_per_account):
        dfColumns = pd.DataFrame(columns=['number_attribute_1', 'number_attribute_2', 'number_attribute_3', 'number_attribute_4', 'number_attribute_5',
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5', 
                                                  'date_attribute_1', 'date_attribute_2', 'date_attribute_3', 'date_attribute_4', 'date_attribute_5'])
        avg = average_num_cp_per_account / len(self.dfCP)
        dfCPId = merge(self.dfAcc, self.dfCP, on='key')
        del dfCPId["key"]
        #print(dfCPId.count())
        dfMCP = dfCPId.sample(frac=avg, replace=True)
        dfMCPCsv = dfMCP.join(dfColumns)
        #dfMCPCsvSorted = dfMCPCsv.sort('account_deal_id', ascending = False)
        self.dfMCPCsvSorted = dfMCPCsv.sort_values(by='acc_id')
        #print(dfMCPCsv)
        #print(dfMCPCsvSorted)
        self.dfMCPCsvSorted.to_csv('mcp_'+self.rdate+'.csv', index=False)
        
    def genterate_config_files(self, cid, rdate, pid, spid, clid, ccy, eff_from_date, eff_to_date):
        if np.isnan(cid):    
            #fake = Faker()
           cid = self.random_int_generator()
        self.cid = cid
        index = []
        index.append(0)
        dfMC = pd.DataFrame(index=index, columns=['key'])
        dfMC['c'] = cid
        dfMC['rdate'] = rdate
        dfMC['pid'] = pid
        dfMC['spid'] = spid
        dfMC['clid']= clid
        dfMC['ccy'] = ccy
        dfMC['eff_from_date'] = eff_from_date
        dfMC['eff_to_date'] = eff_to_date
        del dfMC["key"]
        #print(dfMC)
        dfMC.to_csv('mc_'+self.rdate+'.csv', index=False)
        dfMC.to_csv('nmc_'+self.rdate+'.csv', index=False)  
    
    def generate_check_files(self):
        mf = ['mcco_'+self.rdate+'.csv', 'mor_'+self.rdate+'.csv', 'mto_'+self.rdate+'.csv','mc_'+self.rdate+'.csv', 'mcp_'+self.rdate+'.csv']
        m_num_rows = [len(self.dfMCCOCsv), len(self.dfMORCsv), len(self.dfMTOCsv), len(self.dfMCCsvSorted), len(self.dfMCPCsvSorted) ]
            
        mcList = list(zip(mf, m_num_rows))
        dfMC = pd.DataFrame(mcList, columns=['file', 'num_rows'])
        #print(dfMC)
        dfMC.to_csv('m_check_'+self.rdate+'.csv', index=False)
        
        nmf = ['nmor_'+self.rdate+'.csv']
        nm_num_rows = [len(self.dfNMORCsv)]
        
        nmList = list(zip(nmf, nm_num_rows))
        dfNMC = pd.DataFrame(nmList, columns=['file', 'num_rows'])
        #print(dfNMC)
        dfNMC.to_csv('nm_check_'+self.rdate+'.csv', index=False)
              
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Generate CSV with random values')
    parser.add_argument('--acc', nargs='?', const=1, type=int, default=100,
                        help='account to generate default:100')
    parser.add_argument('--cp', nargs='?', const=1, type=int, default=10,
                        help='cp number (default: 10)')
    parser.add_argument('--c', nargs='?', const=1, type=int, default=10,
                        help='c number (default: 10)')
    parser.add_argument('--t',nargs='?', const=1, type=int, default=6,
                        help='number of t (default: 6)')
    parser.add_argument('--average_num_cp_per_account', nargs='?', const=1, type=int, default=3,
                        help='average_num_cp_per_account  (default: 3)')
    parser.add_argument('--average_num_c_per_account', nargs='?', const=1, type=int, default=5,
                        help='average_num_c_per_account (default: 5)')
    
    #model_config
    parser.add_argument('--cid', nargs='?', const=1, type=int, default=1,
                        help='cid (default: 1)')
    parser.add_argument('--rdate', nargs='?', const=1, type=str, default=datetime.now().strftime("%Y%m%d"),
                        help='The Start Date - format YYYYMMDD (default: today)')
    parser.add_argument('--pid', nargs='?', const=1, type=str, default='P',
                        help='pid (default: P)')
    parser.add_argument('--spid', nargs='?', const=1, type=str, default='SP',
                        help='sub_portfolio_id (default: SP)')
    parser.add_argument('--clid', nargs='?', const=1, type=str, default='1',
                        help='clid (default: 1)')
    parser.add_argument('--ccy', nargs='?', const=1, type=str, default='GBP',
                        help='ccy (default: GBP)')
    parser.add_argument('--eff_from_date', nargs='?', const=1, type=str, default='17000101',
                        help='effective_from_date (default: 1-jan-1700)')
    parser.add_argument('--eff_to_date', nargs='?', const=1, type=str, default='25001231',
                        help='effective_to_date (default: 31-dec-2500)')
    
    parser.add_argument('--s', '--sc', action='append', dest='scn', default=['Base Case', 'BC1', 'BC2', 'WC1', 'WC2'], 
                        help='scenarios (default: NaN)')
        
    print('Starting')
    args = []
    args = parser.parse_args()
    print(args)
    
    mainModule = MainModule(args.rdate)
   
    mainModule.genterate_config_files(args.cid, args.rdate, args.pid, args.spid, args.clid,
                            args.ccy, args.eff_from_date, args.eff_to_date)
     
    mainModule.generate_csv(args.acc, args.cp, args.c, args.t, 
                            args.average_num_cp_per_account, args.average_num_c_per_account, args.scn)
    
    mainModule.generate_check_files()
    print('Done')
    pass
