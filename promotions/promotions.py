import pandas as pd

def load_data(file_path, usecols=None, sep=';'):
    """
    Load and optionally filter CSV data from a given file path.
    
    Parameters:
    - file_path: str, path to the CSV file.
    - usecols: list of str, optional, columns to load from the CSV file.
    - sep: str, delimiter used in the CSV file (default is ';').
    
    Returns:
    - DataFrame containing the loaded and optionally filtered data.
    """
    return pd.read_csv(file_path, usecols=usecols, sep=sep)

def identify_promotions(vendas_df, vendas_produtos_df):
    """
    Identify products sold in promotions based on the criteria that if the product
    is sold at a price lower than usual without a corresponding reduction in cost,
    it indicates a promotion.
    
    Parameters:
    - vendas_df: DataFrame, sales data.
    - vendas_produtos_df: DataFrame, products sold data.
    
    Returns:
    - DataFrame containing information about promotions.
    """
    # Merge the sales and products data on the sale code
    data_merged = pd.merge(vendas_produtos_df, vendas_df, left_on="CodigoVenda", right_on="Codigo")
    
    # Assume 'ValorUnitario' < 'ValorTabela' indicates a promotion if 'PrecoemPromocao' is not null
    data_merged['EmPromocao'] = data_merged.apply(
        lambda row: row['ValorUnitario'] < row['ValorTabela'] if pd.notnull(row.get('PrecoemPromocao', None)) else False, axis=1
    )
    
    # Filter rows where 'EmPromocao' is True
    promotions = data_merged[data_merged['EmPromocao']]
    
    return promotions

def save_to_csv(df, file_path):
    """
    Save the DataFrame to a CSV file.
    
    Parameters:
    - df: DataFrame to save.
    - file_path: str, path where to save the CSV file.
    """
    df.to_csv(file_path, index=False)

def main():
    # Update these paths to where your cleaned data resides
    vendas_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendas.csv'
    vendas_produtos_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendasprodutos.csv'
    promotions_output_path = '/home/jociano/Projects/PromoPredictor/datasetclean/promotion/promotions_identified.csv'
    
    # Define columns to load (adjust according to your data structure)
    vendas_columns = ['Codigo', 'Data', 'TotalPedido']
    vendas_produtos_columns = ['CodigoVenda', 'CodigoProduto', 'ValorUnitario', 'ValorTabela', 'PrecoemPromocao']
    
    # Load cleaned data
    vendas_df = load_data(vendas_clean_path, usecols=vendas_columns)
    vendas_produtos_df = load_data(vendas_produtos_clean_path, usecols=vendas_produtos_columns)
    
    # Identify promotions
    promotions_df = identify_promotions(vendas_df, vendas_produtos_df)
    
    # Save the identified promotions to a new CSV file
    save_to_csv(promotions_df, promotions_output_path)
    
    print(f"Promotions identified and saved to {promotions_output_path}")

if __name__ == "__main__":
    main()
