from flask import Flask

from src.routes import health, jobs, download, parse


PUBLIC_ENDPOINTS = ('health.health_check',)


def register_routes(app: Flask):
    app.register_blueprint(health.bp)
    app.register_blueprint(jobs.bp)
    app.register_blueprint(download.bp)
    app.register_blueprint(parse.bp)
