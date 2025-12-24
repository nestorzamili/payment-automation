from flask import Flask

from src.routes import health, jobs, download, parse, sheets, merchant_ledger, agent_ledger, ledger_summary


PUBLIC_ENDPOINTS = ('health.health_check',)


def register_routes(app: Flask):
    app.register_blueprint(health.bp)
    app.register_blueprint(jobs.bp)
    app.register_blueprint(download.bp)
    app.register_blueprint(parse.bp)
    app.register_blueprint(sheets.bp)
    app.register_blueprint(merchant_ledger.bp)
    app.register_blueprint(agent_ledger.bp)
    app.register_blueprint(ledger_summary.bp)


