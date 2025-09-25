import os
from flask import Flask, current_app, jsonify, render_template, session

from app.utils.cache_utils import configure_cache

def create_app():
    """
    Cria e configura a aplicação Flask com blueprints, configurações e integração com MongoDB.
    """
    app = Flask(__name__)
    
    app.config['SECRET_KEY'] = 'sua_chave_secreta'
    app.config['DEBUG'] = False
    
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
    
    configure_cache(app)
    
    
    # Registro de Blueprints
    try:
        from app.routes.portal_ui import portal_bp
        from app.routes.analise_ui import analise_bp
        from app.routes.recalculo_ui import recalculo_bp
        from app.routes.ipca_correcao_routes import ipca_correcao_bp
        
        blueprints = [portal_bp, analise_bp, recalculo_bp, ipca_correcao_bp]
        for bp in blueprints:
            app.register_blueprint(bp)
    
    except Exception as e:
        print(f"Erro ao carregar o módulo routes.portal: {e}")
        raise e

    

    return app