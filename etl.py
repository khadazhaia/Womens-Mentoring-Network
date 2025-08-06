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
    - Start with First Name
    - If theres no First Name use Telephone
    - If duplicates: add Telephone
    """
    df = rename_columns(df)

    # Start with First Name
    if "First Name" in df.columns:
        df["ClientID"] = df["First Name"].astype(str).str.strip()

        # Check duplicates
        duplicates = df[df["ClientID"].duplicated(keep=False)]

        # If duplicates exist, add Telephone
        if not duplicates.empty and "Telephone" in df.columns:
            df.loc[df["ClientID"].isin(duplicates["ClientID"]),
                   "ClientID"] = df["First Name"].astype(str).str.strip() + "_" + df["Telephone"].astype(str)

        return df
    
    # If theres no First Name use Telephone
    elif "Telephone" in df.columns:
        df["ClientID"] = df["Telephone"].astype(str).str.strip()
        return df
    
    else:
       print("No 'First Name' or 'Telephone' columns found; skipping ClientID creation.")
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
    - Create ClientID from First Name + Telephone
    - Phone calls merged by Telephone (then inherit ClientID)
    """
    # Merge intake + demographics + meetings + salaries on ClientID
    client_data = intake.merge(demographics, on="ClientID", how="outer", suffixes=("_Intake", "_Demo"))
    client_data = client_data.merge(meetings, on="ClientID", how="outer", suffixes=("", "_Meeting"))
    client_data = client_data.merge(salaries, on="ClientID", how="outer", suffixes=("", "_Salary"))

    # Merge phone_calls on Telephone instead of ClientID
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
