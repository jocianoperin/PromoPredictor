from src.api import db

class Produto(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.Integer, unique=True, nullable=False)
    promocoes = db.relationship('Promocao', backref='produto', lazy=True)

class Promocao(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data_promocao = db.Column(db.Date, nullable=False)
    valor_vendido = db.Column(db.Float, nullable=False)
    produto_id = db.Column(db.Integer, db.ForeignKey('produto.id'), nullable=False)