import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd


# Function to remove unnamed columns from the input CSV
def clean_input_csv(input_file):
    df = pd.read_csv(input_file)
    # Drop unnamed columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    # Optionally save the cleaned file to a new CSV
    cleaned_file = "cleaned_input.csv"
    df.to_csv(cleaned_file, index=False)
    print("Unnamed columns removed and cleaned file saved as 'cleaned_input.csv'")
    return cleaned_file


# Function to fetch stock data for a specific ticker and process it
def collecting_data(ticker, start_date, end_date, invested_amount):
    ticker_symbol = f"{ticker}.NS"
    
    # Adjust the end date for yfinance compatibility
    date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    adjusted_end_date = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Fetch the stock data using yfinance
    stock_data = yf.download(ticker_symbol, start=start_date, end=adjusted_end_date)
    data_amount = stock_data["Close"]
    # Extract the 'Close' price and map dates to values
    date_value_dict = {index.strftime('%Y-%m-%d'): value for index, value in data_amount.iloc[:, 0].items()}
    
    # Read weightage from the CSV
    main_data = pd.read_csv("C:/Users/HP/Downloads/alphanalysis assignment/alphanalysis assignment/Stocks.csv")
    if ticker in main_data['Ticker'].values:
        weightage = main_data.loc[main_data['Ticker'] == ticker, 'Weightage'].values[0]
    
    # Calculate the workout amount based on the invested amount and weightage
    workout_amount = invested_amount * weightage
    
    # Update the date_value_dict with workout amounts divided by stock price
    for key in date_value_dict:
        date_value_dict[key] = workout_amount / date_value_dict[key] if date_value_dict[key] != 0 else 0
    
    return date_value_dict


# Function to collect data for all tickers
def collect_data_for_all_tickers(start_date, end_date, invested_amount):
    # Read the main CSV file to get the list of tickers
    main_data = pd.read_csv("Stocks.csv")
    
    # Initialize an empty dictionary to store the results
    ticker_date_value_dict = {}
    
    # Collect data for each ticker and store it in the dictionary
    for ticker in main_data['Ticker']:
        ticker_date_value_dict[ticker] = collecting_data(ticker, start_date, end_date, invested_amount)
        print(f"Data collected for {ticker}")
    
    return ticker_date_value_dict


# Function to add date columns to the DataFrame
def add_date_columns_to_dataframe(df, start_date, end_date):
    # Generate a list of dates between start_date and end_date
    date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    
    # Add new columns for each date to the DataFrame
    for date in date_range:
        df[date] = 0  # Adds a column with 0 as values
    
    return df


# Function to update the DataFrame with date values for each ticker
def update_dataframe_with_date_values(df, ticker_date_value_dict):
    # Loop over each row and update date columns based on ticker
    for index, row in df.iterrows():
        ticker = row['Ticker']
        
        # Check if the ticker has a corresponding date_value_dict
        if ticker in ticker_date_value_dict:
            date_value_dict = ticker_date_value_dict[ticker]
            
            # Update the date columns with the values
            for date in date_value_dict:
                if date in df.columns:
                    df.at[index, date] = date_value_dict[date]
    
    return df


# Main function to execute the entire flow
def main():
    # Input file
    input_file = "Stocks.csv"  # Replace with your actual file name
    
    # Clean the input CSV by removing unnamed columns
    cleaned_input_file = clean_input_csv(input_file)
    
    # Take user input for start date, end date, and invested amount
    start_date = input("Enter the start date (YYYY-MM-DD): ")
    end_date = input("Enter the end date (YYYY-MM-DD): ")
    invested_amount = int(input("Enter the invested amount: "))
    
    # Collect data for all tickers
    ticker_date_value_dict = collect_data_for_all_tickers(start_date, end_date, invested_amount)
    
    # Read the cleaned input file into a DataFrame
    df = pd.read_csv(cleaned_input_file)
    
    # Add date columns to the DataFrame
    df = add_date_columns_to_dataframe(df, start_date, end_date)
    
    # Update the DataFrame with the collected date values
    df = update_dataframe_with_date_values(df, ticker_date_value_dict)
    
    # Save the updated DataFrame to a new CSV
    output_file = "output.csv"
    df.to_csv(output_file, index=False)
    print(f"Updated file saved as {output_file}")


if __name__ == "__main__":
    main()
