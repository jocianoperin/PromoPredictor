from flask import request, jsonify
from .services.data_service import get_promocoes_by_codigo

def init_routes(app):
    @app.route('/api/vendas', methods=['POST'])
    def recebimento_vendas():
        if request.is_json:
            dados = request.get_json()
            # Aqui, você pode chamar uma função de services para processar os dados
            return jsonify({"mensagem": "Dados recebidos", "dados": dados}), 200
        return jsonify({"erro": "Request deve ser JSON"}), 400
    
    @app.route('/api/produto/promocoes/<int:codigo_produto>', methods=['GET'])
    def get_promocoes_produto(codigo_produto):
        try:
            promocoes = get_promocoes_by_codigo(codigo_produto)
            return jsonify(promocoes), 200
        except Exception as e:
            return jsonify({"erro": str(e)}), 500