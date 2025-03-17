import pandas as pd 
import json 
import os 



def make_cdb_df(input_path):
    ls = [] 
    for file in os.listdir(input_path):
        file_path = os.path.join(input_path,file)
        with open(file_path, encoding="utf-8") as f:
            df = json.load(f)
            df = pd.json_normalize(df, record_path=['data'])                                    
            ls.append(df)
    
    ls = [df for df in ls if not df.empty]
    final_df = pd.concat(ls, ignore_index=True)
    return final_df 
    # final_df.to_csv(os.path.join(output_path, "all_cdb.csv"), index=False, encoding="utf-8")

def sanitize_cdb_df(df):
    exclude_cols = [
        'slug',
        'taxation',
        'incomeable_type',
        'company_id',
        'stock_bdr_id',
        'original_type'
    ]
    clean_df = df.drop(columns=exclude_cols)
    return clean_df 

def merge_cdb_kpis(cdb_df: pd.DataFrame, kpi_file_path):
    kpi = pd.read_csv(kpi_file_path,sep=';')
    kpi['cdi_yearly'] = kpi['cdi_yearly'].astype(str).str.strip().str.replace(',', '.').str.replace(r'[^0-9.]', '', regex=True).astype(float)
    kpi['ipca_12'] = kpi['ipca_12'].astype(str).str.strip().str.replace(',', '.').str.replace(r'[^0-9.]', '', regex=True).astype(float)
    final_df = cdb_df.merge(kpi,how='cross')
    return final_df 

def run_transformation():
    output_path = 'data/csv/'
    input_path = 'data/json/'
    kpi_path = 'data/csv/kpis.csv'
    cdbs_df = make_cdb_df(input_path)
    cdbs_df = sanitize_cdb_df(cdbs_df)
    df = merge_cdb_kpis(cdbs_df,kpi_path)
    df.to_csv(f"{output_path}all_cdb.csv", sep=';', index=False)

if __name__ == "__main__":
    run_transformation()