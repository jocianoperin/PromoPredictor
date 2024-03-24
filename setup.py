from setuptools import setup, find_packages

setup(
    name='PromoPredictor',
    version='0.1.0',
    author='Jociano Perin',
    author_email='perinjociano@gmail.com',
    description='O PromoPredictor é um sistema desenvolvido para identificar promoções eficazes em produtos de supermercados com base em dados históricos de vendas',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'mysql-connector-python',
        'flask>=1.1.2',
        # Adicione outras dependências necessárias
    ],
    entry_points={
        'console_scripts': [
            'promopredictor=promopredictor.api.run:main',
        ],
    },
    # inclua outros argumentos necessários
)
