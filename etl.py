import pandas as pd


def extract_excel():
    """
    Load Excel files
    """
    demographics = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/demographics.xlsx")
    intake = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/intake.xlsx")
    meetings = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/meetings.xlsx")
    phone_calls = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/phone_calls.xlsx")
    salaries = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/salaries.xlsx")
    return intake, demographics, salaries, phone_calls, meetings


def rename_columns(df):
    """
    Rename "Name" to "First Name" to match across all files
    """
    if "Name" in df.columns:
        df = df.rename(columns={"Name": "First Name"})
    return df


def client_id(df):
    """
    Create ClientID hierarchy:
    - clean up columns
    - Start with First Name
    - If theres no First Name use Telephone
    - If duplicates: add Telephone
    """
    df = rename_columns(df)

    # Clean up columns
    if "First Name" in df.columns:
        df["First Name"] = df["First Name"].astype(str).str.strip().fillna("")
    if "Telephone" in df.columns:
        df["Telephone"] = df["Telephone"].astype(str).str.strip().fillna("")

    # Start with First Name 
    if "First Name" in df.columns and df["First Name"].any():
        df["ClientID"] = df["First Name"]

        # Find duplicates on ClientID
        duplicates = df[df["ClientID"].duplicated(keep=False)]

        if not duplicates.empty:
            if "Telephone" in df.columns:
                mask = df["ClientID"].isin(duplicates["ClientID"])

                # Add Telephone where available
                df.loc[mask & (df["Telephone"] != ""), "ClientID"] = (
                    df.loc[mask & (df["Telephone"] != ""), "First Name"] + "_" +
                    df.loc[mask & (df["Telephone"] != ""), "Telephone"])

                # Check if duplicates still exist after adding telephone
                remaining_dups = df[df["ClientID"].duplicated(keep=False)]

                if not remaining_dups.empty:
                    # Append index to duplicates without unique telephone
                    df.loc[remaining_dups.index, "ClientID"] = (
                        df.loc[remaining_dups.index, "ClientID"] + "_" +
                        df.loc[remaining_dups.index].groupby("ClientID").cumcount().astype(str))
            else:
                # No telephone column, append index to duplicates
                df.loc[duplicates.index, "ClientID"] = (
                    df.loc[duplicates.index, "ClientID"] + "_" +
                    df.loc[duplicates.index].groupby("ClientID").cumcount().astype(str))

    elif "Telephone" in df.columns and df["Telephone"].any():
        df["ClientID"] = df["Telephone"]

    else:
        print("No 'First Name' column found; skipping ClientID creation.")

    return df


def transform_data(intake, demographics, salaries, phone_calls, meetings):
    """
    - Rename Columns 
    - Create ClientIDs
    - Return cleaned dataframes
    """
    intake = client_id(intake)
    demographics = client_id(demographics)
    salaries = client_id(salaries)
    phone_calls = client_id(phone_calls)
    meetings = client_id(meetings)

    return intake, demographics, salaries, phone_calls, meetings


def merge_export(intake, demographics, salaries, phone_calls, meetings):
    """
    Merge cleaned datasets:
    - Base = demographics (has First Name + Telephone)
    - Merge intake, meetings, salaries by ClientID
    - Merge phone_calls by Telephone
    """
    # Start from demographics
    client_data = demographics.merge(intake, on="ClientID", how="outer", suffixes=("_Demo", "_Intake"))
    client_data = client_data.merge(meetings, on="ClientID", how="outer", suffixes=("", "_Meeting"))
    client_data = client_data.merge(salaries, on="ClientID", how="outer", suffixes=("", "_Salary"))

    # Merge phone_calls by Telephone
    if "Telephone" in phone_calls.columns and "Telephone" in client_data.columns:
        client_data = client_data.merge(
            phone_calls,
            on="Telephone",
            how="outer",
            suffixes=("", "_Phone")
        )

    # Export to Excel
    client_data.to_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/clients_data.xlsx", index=False)
    return client_data


def main():
    # Extract
    intake, demographics, salaries, phone_calls, meetings = extract_excel()

    # Transform
    intake, demographics, salaries, phone_calls, meetings = transform_data(
    intake, demographics, salaries, phone_calls, meetings)

    # Load
    merged_data = merge_export(intake, demographics, salaries, phone_calls, meetings)
    print("✅ Merging complete! File saved as clients_data.xlsx")


if __name__ == "__main__":
    main()
