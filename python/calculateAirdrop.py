import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser(
        description='Scale amounts in a CSV file to reach a target sum while maintaining proportions'
    )
    parser.add_argument(
        'input_file',
        help='Input CSV file (first column: address, second column: amount)'
    )
    parser.add_argument(
        'output_file',
        help='Output CSV file name'
    )
    parser.add_argument(
        'target_sum',
        type=float,
        help='Target sum for the amounts'
    )

    args = parser.parse_args()

    try:
        # Read the CSV without assuming column names
        df = pd.read_csv(args.input_file)
        
        if len(df.columns) < 2:
            raise ValueError("CSV must contain at least 2 columns")

        # Get first two columns
        address_col = df.columns[0]
        amount_col = df.columns[1]

        current_sum = df[amount_col].sum()
        
        if current_sum == 0:
            raise ValueError("Current sum is 0, cannot scale amounts")

        scaling_factor = args.target_sum / current_sum

        # Create new dataframe with scaled amounts
        new_df = df.copy()
        new_df[amount_col] = df[amount_col] * scaling_factor

        # Verify the new sum matches the target
        assert abs(new_df[amount_col].sum() - args.target_sum) < 0.0001, "Sum verification failed"

        # Save to new CSV
        new_df.to_csv(args.output_file, index=False)
        
        print(f"Original sum: {current_sum}")
        print(f"New sum: {new_df[amount_col].sum()}")
        print(f"Scaling factor used: {scaling_factor}")
        
    except FileNotFoundError:
        print(f"Error: Could not find input file '{args.input_file}'")
        exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: Input file '{args.input_file}' is empty")
        exit(1)
    except pd.errors.ParserError:
        print(f"Error: Could not parse '{args.input_file}' as CSV")
        exit(1)
    except ValueError as e:
        print(f"Error: {str(e)}")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()