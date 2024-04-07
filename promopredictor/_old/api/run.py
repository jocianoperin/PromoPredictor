from .routes import app  # Ajuste o import conforme a estrutura do seu projeto

def main():
    app.run(debug=True)  # Coloque debug=False em produção

if __name__ == "__main__":
    main()