import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.logger import get_logger

logger = get_logger(__name__)


def normalize_payment_method(value: Any) -> str:
    if not value:
        return 'ewallet'
    
    str_value = str(value).upper().strip()
    
    if 'FPX' in str_value:
        if 'CORP' in str_value or 'B2B' in str_value:
            return 'FPXC'
        return 'FPX'
    elif 'TNG' in str_value or 'TOUCH' in str_value:
        return 'TNG'
    elif 'BOOST' in str_value:
        return 'BOOST'
    elif 'SHOPEE' in str_value:
        return 'Shopee'
    else:
        return 'ewallet'


def process_kira_file(file_path: Path) -> pd.DataFrame:
    logger.info(f"Processing KIRA: {file_path.name}")
    
    try:
        df = pd.read_excel(file_path)
        
        if df.empty:
            logger.warning(f"Empty file: {file_path.name}")
            return pd.DataFrame()
        
        df.columns = df.columns.str.strip()
        
        column_mapping = {
            'Created On': ['Created On', 'created on', 'createdOn'],
            'Merchant': ['Merchant', 'merchant'],
            'Transaction ID': ['Transaction ID', 'transactionid', 'transactionID'],
            'Merchant Order ID': ['Merchant Order ID', 'merchantOrderNo', 'merchantorderno'],
            'Payment Method': ['Payment Method', 'paymentmethod', 'paymentMethod'],
            'Transaction Amount': ['Transaction Amount', 'transactionamount', 'transactionAmount'],
        }
        
        for target_col, possible_names in column_mapping.items():
            for col in df.columns:
                if col in possible_names:
                    df.rename(columns={col: target_col}, inplace=True)
                    break
        
        result_df = pd.DataFrame()
        result_df['transaction_id'] = df['Transaction ID'].astype(str).str.strip()
        result_df['created_on'] = df['Created On']
        result_df['merchant'] = df['Merchant']
        result_df['merchant_order_id'] = df['Merchant Order ID'].astype(str)
        result_df['payment_method'] = df['Payment Method'].apply(normalize_payment_method)
        result_df['transaction_amount'] = pd.to_numeric(df['Transaction Amount'], errors='coerce').fillna(0)
        
        result_df = result_df[result_df['transaction_id'] != '']
        result_df = result_df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        logger.info(f"KIRA processed: {len(result_df)} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        raise


def process_kira_folder(folder_path: Path) -> pd.DataFrame:
    logger.info(f"Processing KIRA folder: {folder_path}")
    
    if not folder_path.exists():
        logger.warning(f"Folder not found: {folder_path}")
        return pd.DataFrame()
    
    xlsx_files = list(folder_path.glob('**/*.xlsx'))
    
    if not xlsx_files:
        logger.warning(f"No files found: {folder_path}")
        return pd.DataFrame()
    
    all_data = []
    for file_path in xlsx_files:
        try:
            df = process_kira_file(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed: {file_path.name} - {e}")
            continue
    
    if not all_data:
        return pd.DataFrame()
    
    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['transaction_id'], keep='first')
    
    logger.info(f"Total KIRA records: {len(merged_df)}")
    return merged_df
