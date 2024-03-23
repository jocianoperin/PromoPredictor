import pandas as pd

def load_data(file_path, usecols=None, sep=';'):
    """
    Load and optionally filter CSV data from a given file path.
    """
    # Determine if 'Data' is in usecols for parsing dates
    parse_dates = ['Data'] if 'Data' in usecols else None
    return pd.read_csv(file_path, usecols=usecols, sep=sep, parse_dates=parse_dates)

def identify_promotions(vendas_df, vendas_produtos_df):
    """
    Identify products sold in promotions based on stable cost and reduced sale price.
    """
    # Merge the sales and product sales data on sale code
    data_merged = pd.merge(vendas_produtos_df, vendas_df, left_on="CodigoVenda", right_on="Codigo")

    # Sorting by product code and date to ensure chronological order for each product
    data_merged.sort_values(by=['CodigoProduto', 'Data'], inplace=True)

    # Initialize 'EmPromocao' column to False
    data_merged['EmPromocao'] = False
    
    # Loop through each product to identify promotions
    for produto in data_merged['CodigoProduto'].unique():
        produto_df = data_merged[data_merged['CodigoProduto'] == produto]
        for index, row in produto_df.iterrows():
            # Check for previous entries of the product
            prev_entries = produto_df[produto_df['Data'] < row['Data']]
            if not prev_entries.empty:
                avg_cost = prev_entries['ValorCusto'].mean()
                avg_sale_price = prev_entries['ValorUnitario'].mean()

                # Mark as promotion if sale price is reduced while cost remains stable
                if row['ValorUnitario'] < avg_sale_price * 0.95 and abs(row['ValorCusto'] - avg_cost) < avg_cost * 0.05:
                    data_merged.at[index, 'EmPromocao'] = True

    promotions = data_merged[data_merged['EmPromocao']]
    return promotions

def save_to_csv(df, file_path):
    """
    Save the DataFrame to a CSV file.
    """
    df.to_csv(file_path, index=False)

def main():
    # Adjusted paths for cleaned data
    vendas_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendas.csv'
    vendas_produtos_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendasprodutos.csv'
    promotions_output_path = '/home/jociano/Projects/PromoPredictor/datasetclean/promotions_identified.csv'

    # Define columns to load, ensuring 'ValorCusto' is included for vendas_produtos_df
    vendas_columns = ['Codigo', 'Data', 'Hora', 'CodigoCliente', 'TotalPedido']
    vendas_produtos_columns = ['CodigoVenda', 'CodigoProduto', 'ValorUnitario', 'ValorTabela', 'PrecoemPromocao', 'ValorCusto']
    
    # Load the cleaned data
    vendas_df = load_data(vendas_clean_path, usecols=vendas_columns)
    vendas_produtos_df = load_data(vendas_produtos_clean_path, usecols=vendas_produtos_columns)
    
    # Identify promotions
    promotions_df = identify_promotions(vendas_df, vendas_produtos_df)
    
    # Save the identified promotions to a CSV file
    save_to_csv(promotions_df, promotions_output_path)
    
    print(f"Promotions identified and saved to {promotions_output_path}")

if __name__ == "__main__":
    main()
