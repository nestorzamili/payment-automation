"""
KIRA file processor - migrated from KiraProcessor.gs
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import re

from src.core.logger import get_logger

logger = get_logger(__name__)


def normalize_payment_method(value: Any) -> str:
    """
    Normalize payment method to standard values.
    
    Args:
        value: Raw payment method string
        
    Returns:
        Normalized payment method (FPX/FPXC/TNG/BOOST/Shopee/ewallet)
    """
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
    """
    Process a KIRA transaction file.
    
    Args:
        file_path: Path to the KIRA .xlsx file
        
    Returns:
        DataFrame with processed KIRA data
    """
    logger.info(f"Processing KIRA file: {file_path}")
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        
        if df.empty:
            logger.warning(f"Empty KIRA file: {file_path}")
            return pd.DataFrame()
        
        # Normalize column names (case-insensitive matching)
        df.columns = df.columns.str.strip()
        
        # Find columns by possible names
        column_mapping = {
            'Created On': ['Created On', 'created on', 'createdOn'],
            'Merchant': ['Merchant', 'merchant'],
            'Transaction ID': ['Transaction ID', 'transactionid', 'transactionID'],
            'Merchant Order ID': ['Merchant Order ID', 'merchantOrderNo', 'merchantorderno'],
            'Payment Method': ['Payment Method', 'paymentmethod', 'paymentMethod'],
            'Transaction Amount': ['Transaction Amount', 'transactionamount', 'transactionAmount'],
        }
        
        # Create normalized column names
        for target_col, possible_names in column_mapping.items():
            for col in df.columns:
                if col in possible_names:
                    df.rename(columns={col: target_col}, inplace=True)
                    break
        
        # Select and rename columns
        result_df = pd.DataFrame()
        result_df['transaction_id'] = df['Transaction ID'].astype(str).str.strip()
        result_df['created_on'] = df['Created On']
        result_df['merchant'] = df['Merchant']
        result_df['merchant_order_id'] = df['Merchant Order ID'].astype(str)
        result_df['payment_method'] = df['Payment Method'].apply(normalize_payment_method)
        result_df['transaction_amount'] = pd.to_numeric(df['Transaction Amount'], errors='coerce').fillna(0)
        
        # Remove empty transaction IDs
        result_df = result_df[result_df['transaction_id'] != '']
        
        # Remove duplicates based on transaction_id
        initial_count = len(result_df)
        result_df = result_df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial_count - len(result_df)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate transaction IDs from KIRA file")
        
        logger.info(f"Processed KIRA file: {len(result_df)} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing KIRA file {file_path}: {e}")
        raise


def process_kira_folder(folder_path: Path) -> pd.DataFrame:
    """
    Process all KIRA files in a folder.
    
    Args:
        folder_path: Path to folder containing KIRA .xlsx files
        
    Returns:
        DataFrame with all KIRA data merged
    """
    logger.info(f"Processing KIRA folder: {folder_path}")
    
    all_data = []
    
    if not folder_path.exists():
        logger.warning(f"KIRA folder does not exist: {folder_path}")
        return pd.DataFrame()
    
    # Find all .xlsx files
    xlsx_files = list(folder_path.glob('**/*.xlsx'))
    
    if not xlsx_files:
        logger.warning(f"No .xlsx files found in KIRA folder: {folder_path}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(xlsx_files)} KIRA files")
    
    for file_path in xlsx_files:
        try:
            df = process_kira_file(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed to process KIRA file {file_path}: {e}")
            # Don't stop entire pipeline - continue with other files
            continue
    
    if not all_data:
        logger.warning("No data extracted from KIRA folder")
        return pd.DataFrame()
    
    # Merge all dataframes
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates across all files
    initial_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['transaction_id'], keep='first')
    duplicates_removed = initial_count - len(merged_df)
    
    if duplicates_removed > 0:
        logger.warning(f"Removed {duplicates_removed} duplicate transaction IDs across all KIRA files")
    
    logger.info(f"Total KIRA records: {len(merged_df)}")
    return merged_df
