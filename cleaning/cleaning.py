import pandas as pd

def load_data(file_path, usecols):
    """
    Load and optionally filter CSV data from a given file path using specified columns.
    
    Parameters:
    - file_path: str, path to the CSV file.
    - usecols: list of str, columns to load from the CSV file.
    
    Returns:
    - DataFrame containing the loaded and optionally filtered data.
    """
    return pd.read_csv(file_path, usecols=usecols, sep=';', low_memory=False)

def clean_data(vendas_df, vendas_produtos_df):
    """
    Clean the data by removing sales and products with negative or zero values.
    
    Parameters:
    - vendas_df: DataFrame, sales data.
    - vendas_produtos_df: DataFrame, products sold data.
    
    Returns:
    - Tuple of cleaned DataFrames: (cleaned sales data, cleaned products sold data).
    """
    vendas_df_clean = vendas_df[vendas_df['TotalPedido'] > 0]
    vendas_produtos_df_clean = vendas_produtos_df[(vendas_produtos_df['ValorTotal'] > 0) & (vendas_produtos_df['Quantidade'] > 0)]
    
    return vendas_df_clean, vendas_produtos_df_clean

def save_cleaned_data(vendas_df_clean, vendas_produtos_df_clean, vendas_clean_path, vendas_produtos_clean_path):
    """
    Save the cleaned data to new CSV files.
    
    Parameters:
    - vendas_df_clean: DataFrame, cleaned sales data.
    - vendas_produtos_df_clean: DataFrame, cleaned products sold data.
    - vendas_clean_path: str, file path to save cleaned sales data.
    - vendas_produtos_clean_path: str, file path to save cleaned products sold data.
    """
    vendas_df_clean.to_csv(vendas_clean_path, index=False, sep=';')
    vendas_produtos_df_clean.to_csv(vendas_produtos_clean_path, index=False, sep=';')
    print("Cleaned datasets have been saved.")

def main():
    vendas_path = '/home/jociano/Projects/PromoPredictor/dataset/vendasexport.csv'
    vendas_produtos_path = '/home/jociano/Projects/PromoPredictor/dataset/vendasprodutosexport.csv'
    vendas_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendas.csv'
    vendas_produtos_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendasprodutos.csv'

    necessary_vendas_columns = ['Codigo', 'Data', 'Hora', 'CodigoCliente', 'TotalPedido']
    necessary_vendas_produtos_columns = [
        'CodigoVenda', 'CodigoProduto', 'UNVenda', 'Quantidade', 'ValorTabela',
        'ValorUnitario', 'ValorTotal', 'Desconto', 'CodigoSecao', 'CodigoGrupo',
        'CodigoSubGrupo', 'CodigoFabricante', 'ValorCusto', 'ValorCustoGerencial', 'PrecoemPromocao'
    ]

    vendas_df = load_data(vendas_path, usecols=necessary_vendas_columns)
    vendas_produtos_df = load_data(vendas_produtos_path, usecols=necessary_vendas_produtos_columns)
    
    vendas_df_clean, vendas_produtos_df_clean = clean_data(vendas_df, vendas_produtos_df)
    
    save_cleaned_data(vendas_df_clean, vendas_produtos_df_clean, vendas_clean_path, vendas_produtos_clean_path)

if __name__ == "__main__":
    main()
