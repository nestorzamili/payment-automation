from sqlalchemy import func

from src.core import load_accounts
from src.core.database import get_session, init_db
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.core.models import KiraTransaction
from src.parser.m1 import M1Parser
from src.parser.axai import AxaiParser
from src.parser.kira import KiraParser
from src.services.kira_pg import init_kira_pg
from src.services.deposit import init_deposit
from src.services.merchant_ledger import MerchantLedgerService
from src.services.agent_ledger import AgentLedgerService
from src.services.parameters import ParameterService

logger = get_logger(__name__)


def run_parse_job(run_id: str) -> dict:
    init_db()
    
    _parse_kira_files(run_id)
    _parse_pg_files(run_id)
    
    ParameterService.sync_from_sheet()
    
    init_kira_pg()
    init_deposit()
    _init_ledgers(MerchantLedgerService, AgentLedgerService)
    
    result = _get_dropdown_data()
    _setup_dropdowns(result['merchants'], result['periods'])
    
    logger.info(f"Parse job completed (run_id: {run_id})")
    return result


def _parse_kira_files(run_id: str):
    kira_dir = PROJECT_ROOT / 'data' / 'kira'
    if not kira_dir.exists():
        return
    
    try:
        parser = KiraParser()
        result = parser.process_directory(kira_dir, run_id=run_id)
        logger.info(f"Kira: parsed {result['total_transactions']} transactions")
    except Exception as e:
        logger.error(f"Kira parse error: {e}")


def _parse_pg_files(run_id: str):
    accounts = load_accounts()
    pg_accounts = [a for a in accounts if a['platform'] in ('m1', 'axai')]
    
    parsers = {'m1': M1Parser, 'axai': AxaiParser}
    
    for account in pg_accounts:
        label = account['label']
        platform = account['platform']
        data_dir = PROJECT_ROOT / 'data' / label
        
        if not data_dir.exists():
            continue
        
        if platform not in parsers:
            continue
        
        try:
            parser = parsers[platform]()
            result = parser.process_directory(data_dir, label, run_id=run_id)
            logger.info(f"{label}: parsed {result['total_transactions']} transactions")
        except Exception as e:
            logger.error(f"{label} parse error: {e}")


def _init_ledgers(MerchantLedgerService, AgentLedgerService):
    session = get_session()
    
    try:
        merchants = session.query(KiraTransaction.merchant).distinct().all()
        merchants = [m[0] for m in merchants if m[0]]
        
        year_months = session.query(
            func.substr(KiraTransaction.transaction_date, 1, 7).label('ym')
        ).distinct().all()
        year_months = [ym[0] for ym in year_months if ym[0]]
        
        session.close()
        
        if not merchants or not year_months:
            logger.info("No transactions found, skipping ledger init")
            return
        
        merchant_service = MerchantLedgerService()
        agent_service = AgentLedgerService()
        
        periods = []
        for ym in sorted(year_months):
            year = int(ym[:4])
            month = int(ym[5:7])
            for merchant in sorted(merchants):
                periods.append((merchant, year, month))
        
        for merchant, year, month in periods:
            try:
                merchant_service.init_from_transactions(merchant, year, month)
                agent_service.init_from_transactions(merchant, year, month)
            except Exception as e:
                logger.error(f"Failed to init ledger for {merchant} {year}-{month}: {e}")
        
        logger.info(f"Initialized ledgers for {len(periods)} merchant-periods")
        
    except Exception as e:
        logger.error(f"Failed to initialize ledgers: {e}")


def _get_dropdown_data() -> dict:
    session = get_session()
    
    try:
        merchants = session.query(KiraTransaction.merchant).distinct().all()
        merchants = sorted([m[0] for m in merchants if m[0]])
        
        year_months = session.query(
            func.substr(KiraTransaction.transaction_date, 1, 7).label('ym')
        ).distinct().all()
        
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        periods = []
        for rec in year_months:
            if rec.ym:
                year = rec.ym[:4]
                month_num = int(rec.ym[5:7])
                periods.append(f"{months[month_num-1]} {year}")
        
        periods.sort(key=lambda x: (x.split()[1], months.index(x.split()[0])))
        
        return {
            'merchants': merchants,
            'periods': periods
        }
    finally:
        session.close()


def _setup_dropdowns(merchants: list, periods: list):
    try:
        from src.services.client import SheetsClient
        
        client = SheetsClient()
        
        client.set_dropdown('Kira PG', 'B1', periods)
        
        client.set_dropdown('Deposit', 'B1', merchants)
        client.set_dropdown('Deposit', 'B2', periods)
        
        client.set_dropdown('Merchants Balance & Settlement Ledger', 'B1', merchants)
        client.set_dropdown('Merchants Balance & Settlement Ledger', 'B2', periods)
        
        client.set_dropdown('Agents Balance & Settlement Ledger', 'B1', merchants)
        client.set_dropdown('Agents Balance & Settlement Ledger', 'B2', periods)
        
        logger.info("Dropdowns setup completed")
        
    except Exception as e:
        logger.error(f"Failed to setup dropdowns: {e}")
