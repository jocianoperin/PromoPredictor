from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

db = SQLAlchemy()

class VendasProdutoExport(db.Model):
    __tablename__ = 'vendasprodutosexport'
    id = db.Column(db.Integer, primary_key=True)
    CodigoVenda = db.Column(db.Integer, nullable=False)
    CodigoProduto = db.Column(db.Integer, nullable=False)
    UNVenda = db.Column(db.String(255))
    Quantidade = db.Column(db.Integer)
    ValorTabela = db.Column(db.Float)
    ValorUnitario = db.Column(db.Float)
    ValorTotal = db.Column(db.Float)
    Desconto = db.Column(db.Float)
    CodigoSecao = db.Column(db.Integer)
    CodigoGrupo = db.Column(db.Integer)
    CodigoSubGrupo = db.Column(db.Integer)
    CodigoFabricante = db.Column(db.Integer)
    ValorCusto = db.Column(db.Float)
    ValorCustoGerencial = db.Column(db.Float)
    Cancelada = db.Column(db.Boolean)
    PrecoemPromocao = db.Column(db.Boolean)

class VendasExport(db.Model):
    __tablename__ = 'vendasexport'
    id = db.Column(db.Integer, primary_key=True)
    Codigo = db.Column(db.Integer, nullable=False)
    Data = db.Column(db.Date, nullable=False)
    Hora = db.Column(db.Time)
    CodigoCliente = db.Column(db.Integer)
    Status = db.Column(db.String(255))
    TotalPedido = db.Column(db.Float)
    Endereco = db.Column(db.String(255))
    Numero = db.Column(db.String(255))
    Bairro = db.Column(db.String(255))
    Cidade = db.Column(db.String(255))
    UF = db.Column(db.String(2))
    CEP = db.Column(db.String(9))
    TotalCusto = db.Column(db.Float)
    Rentabilidade = db.Column(db.Float)

class PromotionsIdentified(db.Model):
    __tablename__ = 'promotions_identified'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    CodigoProduto = db.Column(db.Integer, nullable=False)
    Data = db.Column(db.Date, nullable=False)
    ValorUnitario = db.Column(db.Float, nullable=False)
    ValorTabela = db.Column(db.Float, nullable=False)
    __table_args__ = (db.UniqueConstraint('CodigoProduto', 'Data', name='unique_promocao'),)

class SalesIndicators(db.Model):
    __tablename__ = 'sales_indicators'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    CodigoProduto = db.Column(db.Integer, nullable=False)
    Data = db.Column(db.Date, nullable=False)
    QuantidadeTotal = db.Column(db.Integer)
    ValorTotalVendido = db.Column(db.Float)
    __table_args__ = (db.UniqueConstraint('CodigoProduto', name='unique_indicator'),)