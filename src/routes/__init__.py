from flask import Flask

from src.routes import health, jobs, download, parse, merchant_balance, agent_balance, ledger_summary, kira_pg, deposit


PUBLIC_ENDPOINTS = ('health.health_check',)


def register_routes(app: Flask):
    app.register_blueprint(health.bp)
    app.register_blueprint(jobs.bp)
    app.register_blueprint(download.bp)
    app.register_blueprint(parse.bp)
    app.register_blueprint(merchant_balance.bp)
    app.register_blueprint(agent_balance.bp)
    app.register_blueprint(ledger_summary.bp)
    app.register_blueprint(kira_pg.bp)
    app.register_blueprint(deposit.bp)
