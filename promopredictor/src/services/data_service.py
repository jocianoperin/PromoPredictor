from src.api import db
from src.models import Produto, Promocao

def get_promocoes_by_codigo(codigo_produto):
    produto = Produto.query.filter_by(codigo=codigo_produto).first()
    if produto:
        promocoes = [{
            "data": promo.data_promocao,
            "valor_vendido": promo.valor_vendido
        } for promo in produto.promocoes]
        return promocoes
    return []